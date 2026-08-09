map_char = {
    "THORN": "Th",
    "ETH": "D",
    "thorn": "th",
    "eth": "d",
    "aelig": "ae",
    "AElig": "AE",
    "oelig": "oe",
    "OElig": "OE",
    "#64256": "ff",
    "#64257": "fi",
    "#64258": "fl",
    "#64259": "ffi",
    "#64260": "ffl",
}

map_pnct = {
    "nbsp": " ",
    "zwj": " ",
    "zwnj": " ",
    "lsquo": "`",
    "rsquo": "'",
    "ldquo": "``",
    "rdquo": "''",
    "bdquo": "''",
    "bsquo": "'",
    "sbquo": "'",
    "laquo": "<<",
    "raquo": ">>",
    "#8208": "--",
    "#8209": "-",
    "#8210": "--",
    "ndash": "--",
    "mdash": "---",
    "#8213": "---",
    "hellip": "...",
    "lsaquo": "<",
    "rsaquo": ">",
}

map_symb = {
    "#8723": "-/+",
    "#8592": "<--",
    "#8594": "-->",
    "#8596": "<-->",
    "#8729": ".",
    "#8764": "~",
    "#8800": "!=",
    "#8804": "<=",
    "#8805": ">=",
}

map_typo = {
    "#121484": "&#305;",
}

map_list = [map_char, map_pnct, map_symb, map_typo]

ASCII_CUST_MAP = {}
for m in map_list:
    ASCII_CUST_MAP.update(m)
