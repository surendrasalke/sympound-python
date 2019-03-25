import os
import json
from sympound import Sympound
from collections import Counter
from nltk.tokenize import word_tokenize

import platform

distancefun = None
if platform.system() != "Windows":
    from pyxdameraulevenshtein import damerau_levenshtein_distance

    distancefun = damerau_levenshtein_distance
else:
    from jellyfish import levenshtein_distance

    distancefun = levenshtein_distance

ssc = Sympound(maxDictionaryEditDistance=3)


def test():
    print(ssc.load_dictionary("example-dict.txt", term_index=0, count_index=1))

    # ssc.save_pickle("symspell.pickle")
    while True:
        query = input("Enter word to spell correct: ")
        print(ssc.lookup_compound("local_speller", input_string=query))


# test()
class SpellerUtil():
    @staticmethod
    def create_histograpm(input_string):
        """ Create freq table"""

        letter_counts = Counter(word_tokenize(input_string))
        freq_dict = dict()
        for letter, count in dict(letter_counts).items():
            # print(letter, count)
            if len(letter) > 3:
                freq_dict[letter] = count
        return freq_dict


speller_util = SpellerUtil()
sympound = Sympound()


class Speller():
    def train(self, speller_id, incremental_train, corpus):
        """Train speller for corpus"""

        if not incremental_train:
            sympound.delete_model(speller_id)
        freq_dict = speller_util.create_histograpm(corpus)
        for key, value in freq_dict.items():
            sympound.create_dictionary_entry(speller_id, key, value)

        return True

    def correct(self, speller_id, query):
        """spell correct sentence"""
        sympound.lookup_compound(speller_id, query)


if __name__ == "__main__":
    speller_util = SpellerUtil()
    speller = Speller()
    speller_id = "splr-1589"
    corpus = "God is Great! I won a lottery. Thanks God!"
    print(speller_util.create_histograpm(corpus))
    speller.train(speller_id, False, corpus)

