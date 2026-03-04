import html
import re

def u2html5(unicode_string):
    # +1 for gemini...
    char_to_entity = {char: f"&{name}" for name, char in html.entities.html5.items() if len(char) == 1}
    translation_table = {ord(char): entity for char, entity in char_to_entity.items()}
    html5_string = unicode_string.translate(translation_table)
    return html5_string

