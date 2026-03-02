"""
Convert the 2025 Annual Report to a standalone HTML file.
Embeds images as base64 for single-file sharing.
"""

import base64
import re
from pathlib import Path
import markdown

# Paths
REPORT_PATH = Path("data/generated_content/pm_market_report_2025_annual_with_charts.md")
CHARTS_DIR = Path("data/generated_content/charts/2025_annual")
OUTPUT_HTML = Path("data/generated_content/2025_Annual_Market_Report.html")


def embed_images(html_content: str, base_dir: Path) -> str:
    """Replace image src with base64 embedded data."""
    generated_content_dir = Path("data/generated_content")

    def replace_image(match):
        img_path = match.group(1)
        # Handle relative paths from generated_content directory
        full_path = generated_content_dir / img_path

        if full_path.exists():
            with open(full_path, 'rb') as f:
                img_data = base64.b64encode(f.read()).decode('utf-8')
            ext = full_path.suffix.lower()
            mime = 'image/png' if ext == '.png' else 'image/jpeg'
            return f'src="data:{mime};base64,{img_data}"'
        else:
            print(f"  [!] Image not found: {full_path}")
            return match.group(0)

    # Replace src="..." patterns
    pattern = r'src="([^"]+\.(?:png|jpg|jpeg|gif))"'
    return re.sub(pattern, replace_image, html_content, flags=re.IGNORECASE)


def convert_to_html():
    """Convert markdown report to standalone HTML."""

    print("=" * 70)
    print("CONVERTING REPORT TO HTML")
    print("=" * 70)

    # Read markdown
    with open(REPORT_PATH, 'r', encoding='utf-8') as f:
        md_content = f.read()

    print(f"Read markdown: {len(md_content):,} characters")

    # Convert markdown to HTML
    html_body = markdown.markdown(
        md_content,
        extensions=['tables', 'fenced_code', 'toc', 'nl2br']
    )

    # Create full HTML document with professional styling
    html_template = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dubai Real Estate Market Report 2025 | Property Monitor</title>
    <style>
        :root {{
            --primary: #1a365d;
            --secondary: #2b6cb0;
            --accent: #38a169;
            --gold: #d69e2e;
            --text: #2d3748;
            --light-bg: #f7fafc;
            --border: #e2e8f0;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.7;
            color: var(--text);
            background: #fff;
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 60px;
        }}

        /* Header styling */
        h1 {{
            font-size: 2.5rem;
            color: var(--primary);
            border-bottom: 4px solid var(--gold);
            padding-bottom: 15px;
            margin-bottom: 30px;
        }}

        h2 {{
            font-size: 1.8rem;
            color: var(--primary);
            margin-top: 50px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid var(--border);
        }}

        h3 {{
            font-size: 1.4rem;
            color: var(--secondary);
            margin-top: 35px;
            margin-bottom: 15px;
        }}

        h4 {{
            font-size: 1.2rem;
            color: var(--text);
            margin-top: 25px;
            margin-bottom: 10px;
        }}

        /* Paragraph and text */
        p {{
            margin-bottom: 16px;
            text-align: justify;
        }}

        strong {{
            color: var(--primary);
        }}

        /* Lists */
        ul, ol {{
            margin-bottom: 20px;
            padding-left: 30px;
        }}

        li {{
            margin-bottom: 8px;
        }}

        /* Tables */
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 0.95rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}

        th {{
            background: var(--primary);
            color: white;
            padding: 14px 12px;
            text-align: left;
            font-weight: 600;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid var(--border);
        }}

        tr:nth-child(even) {{
            background: var(--light-bg);
        }}

        tr:hover {{
            background: #edf2f7;
        }}

        /* Images/Charts */
        img {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 30px auto;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }}

        /* Blockquotes for highlights */
        blockquote {{
            background: linear-gradient(135deg, var(--light-bg), #edf2f7);
            border-left: 5px solid var(--gold);
            padding: 20px 25px;
            margin: 25px 0;
            font-style: italic;
        }}

        /* Code/data blocks */
        code {{
            background: var(--light-bg);
            padding: 2px 6px;
            border-radius: 4px;
            font-family: 'Consolas', monospace;
            font-size: 0.9em;
        }}

        pre {{
            background: var(--primary);
            color: #e2e8f0;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            margin: 20px 0;
        }}

        pre code {{
            background: none;
            padding: 0;
            color: inherit;
        }}

        /* Key metrics highlight boxes */
        .metric-box {{
            display: inline-block;
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            color: white;
            padding: 15px 25px;
            margin: 10px;
            border-radius: 8px;
            text-align: center;
        }}

        /* Print styles */
        @media print {{
            body {{
                padding: 20px;
                font-size: 11pt;
            }}

            h1 {{ font-size: 24pt; }}
            h2 {{ font-size: 18pt; page-break-before: always; }}
            h3 {{ font-size: 14pt; }}

            img {{
                max-width: 100%;
                page-break-inside: avoid;
            }}

            table {{
                page-break-inside: avoid;
            }}
        }}

        /* Footer */
        .footer {{
            margin-top: 60px;
            padding-top: 20px;
            border-top: 2px solid var(--border);
            text-align: center;
            color: #718096;
            font-size: 0.9rem;
        }}
    </style>
</head>
<body>
    <header>
        <div style="text-align: center; margin-bottom: 40px;">
            <h1 style="border: none; margin-bottom: 10px;">DUBAI REAL ESTATE MARKET REPORT</h1>
            <p style="font-size: 1.3rem; color: var(--secondary); font-weight: 600;">Annual Review 2025</p>
            <p style="color: #718096;">Property Monitor | Exclusive Market Intelligence</p>
        </div>
    </header>

    <main>
        {html_body}
    </main>

    <footer class="footer">
        <p><strong>Property Monitor</strong> | Dubai Real Estate Intelligence</p>
        <p>Report Generated: January 2026</p>
        <p style="font-size: 0.8rem; margin-top: 10px;">
            This report is based on official Dubai Land Department transaction data.<br>
            All figures verified through Property Monitor analytics.
        </p>
    </footer>
</body>
</html>'''

    # Embed images as base64
    print("Embedding images...")
    html_final = embed_images(html_template, CHARTS_DIR)

    # Count embedded images
    embedded_count = html_final.count('data:image/')
    print(f"  Embedded {embedded_count} images")

    # Write HTML file
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html_final)

    print(f"\n{'=' * 70}")
    print("CONVERSION COMPLETE")
    print(f"{'=' * 70}")
    print(f"Output: {OUTPUT_HTML}")
    print(f"Size: {len(html_final):,} characters ({len(html_final)/1024/1024:.1f} MB)")
    print(f"\nOpen in browser: file:///{OUTPUT_HTML.absolute()}")

    return OUTPUT_HTML


if __name__ == "__main__":
    convert_to_html()
