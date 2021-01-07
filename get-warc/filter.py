from selectolax.parser import HTMLParser

from config import MIN_AMOUNT


def get_text_from_html(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css(u'script'):
        tag.decompose()
    for tag in tree.css(u'style'):
        tag.decompose()

    text = tree.body.text(separator=u'\n')
    return text


def get_longest_sentence(text):
    text = text.replace('\t', '')
    return sorted(map(lambda t: t.strip(), text.split('\n')), key=len)[-MIN_AMOUNT:]


def filter_text(text):
    longest_lines = get_longest_sentence(text)
    text = text.split('\n')
    return len(text) < MIN_AMOUNT or any([len(line) < 30 for line in longest_lines])
