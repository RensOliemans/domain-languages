import cdx_toolkit
from langdetect import detect
from selectolax.parser import HTMLParser

print('Initialising CC Fetcher')
cdx = cdx_toolkit.CDXFetcher(source='cc')
URL = '*.nl'


def get_objects(url, limit=100):
	for obj in cdx.iter(url, limit=limit, filter=["!~robots.txt", 'mime:text/html']):
		yield obj


def main():
	print('Fetching items')
	items = get_objects(URL, 15)

	languages = dict()

	for i, item in enumerate(items):
		url = item.data['url']
		print("url {}: {}".format(i, url))

		parsed = get_text_selectolax(item.content)
		if parsed is None or filter_text(parsed):
			continue

		longest_sentences = get_longest_sentence(parsed)
		print(longest_sentences)
		language = detect(' '.join(longest_sentences))
		print(f"language: {language}\n")

		if language in languages:
			languages[language] += 1
		else:
			languages[language] = 1

	print(languages)



def get_text_selectolax(html):
	tree = HTMLParser(html)

	if tree.body is None:
		return None

	for tag in tree.css(u'script'):
		tag.decompose()
	for tag in tree.css(u'style'):
		tag.decompose()

	text = tree.body.text(separator=u'\n')
	return text


AMOUNT = 5


def get_longest_sentence(text):
	text = text.replace('\t', '')
	return sorted(map(lambda t: t.strip(), text.split('\n')), key=len)[-AMOUNT:]


def filter_text(text):
	longest_lines = get_longest_sentence(text)
	text = text.split('\n')
	return len(text) < AMOUNT or any([len(line) < 30 for line in longest_lines])


main()
