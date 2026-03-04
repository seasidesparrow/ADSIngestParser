import html.entities
import re

def convert_unicode_to_html5_entities(text):
    """
    Converts non-ASCII Unicode characters to HTML5 named entities 
    or numeric entities.
    """
    # Create a mapping from Unicode codepoint to named entity, including semicolon
    codepoint_to_entity = {
        code: f"&{name}" 
        for name, code in html.entities.html5.items()
    }
    
    # Use a regular expression to find all non-ASCII characters
    # and replace them using the mapping
    non_ascii_pattern = re.compile("[\u0080-\U0010FFFF]")

    def replace_match(match):
        char = match.group(0)
        code_point = ord(char)
        # Try to find a named entity, otherwise use numeric entity
        return codepoint_to_entity.get(char, f"&#{code_point};")

    return non_ascii_pattern.sub(replace_match, text)

def main():
    turkish_text = "Türkçe metin örneği: İstanbul, İzmir, Adana, Samsun. İ, ı, Ş, ş, Ğ, ğ, Ü, ü, Ö, ö, Ç, ç"
    print(f"Original: {turkish_text}\n\n")

    encoded_turkish = convert_unicode_to_html5_entities(turkish_text)
    print(f"Encoded:  {encoded_turkish}")

if __name__ == "__main__":
    main()
