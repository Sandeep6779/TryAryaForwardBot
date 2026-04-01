import re
_NOISE_RE = [
    re.compile(r'(?i)\b(?:360|480|720|1080|2160|4k)[pi]\b'),
    re.compile(r'(?i)\b(?:x264|x265|h\.?264|h\.?265|hevc|avc|aac|mp[34]|m4a|m4v|m4b|mkv|avi|mov|wmv|flv|flac|opus|ogg|wav|webm|3gp|mts|m2ts)\b'),
    re.compile(r'(?i)\b\d+(?:\.\d+)?\s*(?:mb|gb|kb)\b'),
    re.compile(r'(?i)\b(?:s[0-9]{1,2}e[0-9]{1,2})(?=\s|$)'),
    re.compile(r'(?i)\b(?:copy|final|v\d+|new|latest|audio|track)\b'),
]
def _clean(text):
    for rx in _NOISE_RE:
        text = rx.sub(' ', text)
    text = re.sub(r'[_#\.]', ' ', text)
    return text

def _extract(text):
    c = _clean(text)
    
    # 2. explicit range with '-', 'to' etc
    r = re.search(r'(?<!\d)(?:ep|episode|e|ch|chapter|part|एपिसोड|भाग)?[\s\-\:]*(\d{1,4}(?:(?:\s*[-\u2013\u2014]|(?i:\s+to\s+))\s*\d{1,4})+)(?!\d)', c, re.IGNORECASE)
    if r:
        nums = [int(x) for x in re.findall(r'\d+', r.group(1))]
        if max(nums) < 5000 and len(nums) >= 2 and nums == sorted(nums) and len(set(nums)) == len(nums):
            if (nums[-1] - nums[0]) < 1000:
                return f"Rule 2 (range): {nums}"
            
    kw = re.search(r'(?i)\b(?:ep|episode|e|ch|chapter|part|एपिसोड|भाग)[\s\-\:]*(\d{1,4})(?!\d)', c)
    if kw:
        n = int(kw.group(1))
        if 0 < n < 5000:
            return f"Rule 3 (keyword): {n}"
            
    c2 = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', c)
    c2 = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', c2)
    nums = [int(x) for x in re.findall(r'(?<!\d)(\d{1,4})(?!\d)', c2) if 0 < int(x) < 5000]
    if nums:
        return f"Rule 4 (nums fallback): {max(nums)}"
    return 'None'

tests = [
	'Ep 2570 - Tanish ka hmla',
    'Ep 885 - 9 Shaitano ka war',
    'Ep 1126 - 8 Prachana ki mulakat',
    'Ep 1-10',
    'Ep 15 to 300',
    'Ep 2679 - Teer se samna'
]
for t in tests:
	print(f"[{t}] -> {_extract(t)}")
