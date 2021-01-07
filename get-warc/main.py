from langdetect import detect

from fetch import Fetcher
from filter import filter_text, get_longest_sentence, get_text_from_html

URL = '*.nl'


def main():
	print('Fetching items')
	fetcher = Fetcher(URL, 15)

	languages = dict()

	for i, item in enumerate(fetcher.objects):
		url = item.data['url']
		print("url {}: {}".format(i, url))

		parsed = get_text_from_html(item.content)
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


main()
