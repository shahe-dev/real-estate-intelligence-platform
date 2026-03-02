# config/validation_rules.py

"""
Data Quality Rules - Surgical, not hammer approach
Based on Dubai Land Department data patterns
"""

class ValidationRules:
    """
    Conservative validation - only filter obvious errors
    """
    
    # Price validation (AED)
    PRICE_RULES = {
        'sales': {
            'Unit': {
                'min': 50_000,      # Studio in far areas
                'max': 100_000_000,  # Ultra luxury penthouses
                'warn_below': 200_000,  # Flag but don't exclude
                'warn_above': 50_000_000
            },
            'Villa': {
                'min': 200_000,
                'max': 400_000_000,  # Palm Jumeirah mega-mansions
                'warn_below': 500_000,
                'warn_above': 100_000_000
            },
            'Townhouse': {
                'min': 100_000,
                'max': 100_000_000,
                'warn_below': 300_000,
                'warn_above': 30_000_000
            },
            'Land': {
                'min': 100_000,
                'max': 500_000_000,
                'warn_below': 500_000,
                'warn_above': 200_000_000
            },
            'Building': {
                'min': 500_000,
                'max': 1_000_000_000,
                'warn_below': 2_000_000,
                'warn_above': 500_000_000
            }
        },
        'mortgages': {
            'min': 50_000,
            'max': 500_000_000,
        },
        'gifts': {
            'min': 1,  # Gifts can be symbolic
            'max': 500_000_000,
        }
    }
    
    # Price per sqm (AED)
    PRICE_PER_SQM = {
        'min': 100,        # Very far areas or old properties
        'max': 150_000,    # Ultra luxury (Bulgari, One Za'abeel)
        'typical_min': 800,
        'typical_max': 50_000,
        'warn_below': 500,
        'warn_above': 80_000
    }
    
    # Area sizes (square meters)
    AREA_RULES = {
        'Studio': {'min': 20, 'max': 100, 'typical': (25, 55)},
        '1 B/R': {'min': 35, 'max': 200, 'typical': (45, 90)},
        '2 B/R': {'min': 60, 'max': 300, 'typical': (80, 150)},
        '3 B/R': {'min': 100, 'max': 500, 'typical': (120, 250)},
        '4 B/R': {'min': 150, 'max': 1000, 'typical': (180, 400)},
        '5 B/R': {'min': 200, 'max': 2000, 'typical': (250, 600)},
        'Villa': {'min': 150, 'max': 10000, 'typical': (300, 2000)},
        'Land': {'min': 50, 'max': 100000, 'typical': (200, 5000)},
    }
    
    # Date validation
    DATE_RULES = {
        'min_year': 2002,  # DLD established
        'max_year': 2025,  # Current + buffer
    }
    
    # Premium areas (for luxury focus 5M+)
    LUXURY_AREAS = [
        'Palm Jumeirah',
        'Burj Khalifa',
        'Dubai Marina',
        'Downtown Dubai',
        'Emirates Hills',
        'Dubai Hills Estate',
        'Jumeirah Bay Island',
        'Bluewaters Island',
        'Business Bay',
        'Jumeirah Beach Residence',
        'Al Barari'
    ]
    
    @staticmethod
    def validate_sale_price(price, property_type, property_subtype=None):
        """
        Returns: (is_valid, warning_message, quality_score)
        quality_score: 1.0 = excellent, 0.5 = suspicious, 0.0 = invalid
        """
        if price is None or price <= 0:
            return False, "Missing or zero price", 0.0
        
        rules = ValidationRules.PRICE_RULES['sales'].get(property_type, {})
        
        if not rules:
            # Unknown property type - be permissive
            return True, None, 0.7
        
        # Hard limits
        if price < rules['min']:
            return False, f"Price too low: {price:,.0f} < {rules['min']:,.0f}", 0.0
        
        if price > rules['max']:
            return False, f"Price too high: {price:,.0f} > {rules['max']:,.0f}", 0.0
        
        # Warning thresholds
        if price < rules.get('warn_below', 0):
            return True, f"Unusually low: {price:,.0f}", 0.6
        
        if price > rules.get('warn_above', float('inf')):
            return True, f"Unusually high: {price:,.0f}", 0.6
        
        return True, None, 1.0
    
    @staticmethod
    def validate_price_per_sqm(price_sqm, property_type):
        """Validate price per square meter"""
        if price_sqm is None or price_sqm <= 0:
            return True, "Missing price/sqm", 0.5  # Not critical
        
        rules = ValidationRules.PRICE_PER_SQM
        
        if price_sqm < rules['min']:
            return False, f"Price/sqm too low: {price_sqm:,.0f}", 0.0
        
        if price_sqm > rules['max']:
            return False, f"Price/sqm too high: {price_sqm:,.0f}", 0.0
        
        if price_sqm < rules['warn_below']:
            return True, f"Unusually low price/sqm: {price_sqm:,.0f}", 0.6
        
        if price_sqm > rules['warn_above']:
            return True, f"Unusually high price/sqm: {price_sqm:,.0f}", 0.6
        
        return True, None, 1.0
    
    @staticmethod
    def is_luxury_property(price, area_name=None):
        """Check if property qualifies as luxury (5M+ AED)"""
        return price >= 5_000_000
    
    @staticmethod
    def calculate_quality_score(row):
        """
        Calculate overall data quality score for a transaction
        1.0 = Perfect, 0.0 = Invalid
        """
        scores = []
        
        # Price validation
        if row['trans_group_en'] == 'Sales' and row['actual_worth']:
            is_valid, msg, score = ValidationRules.validate_sale_price(
                row['actual_worth'], 
                row['property_type_en']
            )
            if not is_valid:
                return 0.0  # Invalid
            scores.append(score)
        
        # Price per sqm
        if row['meter_sale_price']:
            is_valid, msg, score = ValidationRules.validate_price_per_sqm(
                row['meter_sale_price'],
                row['property_type_en']
            )
            if not is_valid:
                return 0.0
            scores.append(score)
        
        # Date validation
        try:
            year = int(row['instance_date'].split('-')[-1])
            if year < ValidationRules.DATE_RULES['min_year'] or \
               year > ValidationRules.DATE_RULES['max_year']:
                return 0.0
            scores.append(1.0)
        except:
            scores.append(0.5)
        
        # Required fields
        required_fields = ['transaction_id', 'area_name_en', 'property_type_en']
        if all(row.get(f) for f in required_fields):
            scores.append(1.0)
        else:
            scores.append(0.3)
        
        return sum(scores) / len(scores) if scores else 0.5