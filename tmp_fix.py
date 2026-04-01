import glob

for fname in glob.glob("plugins/*.py"):
    with open(fname, "r", encoding="utf-8") as f:
        text = f.read()
    
    # Replace parse_mode="html" with ParseMode.HTML
    if 'parse_mode="html"' in text or "parse_mode='html'" in text:
        text = text.replace('parse_mode="html"', 'parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML')
        text = text.replace("parse_mode='html'", 'parse_mode=__import__("pyrogram.enums", fromlist=["ParseMode"]).ParseMode.HTML')
        with open(fname, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Fixed {fname}")
