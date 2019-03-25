import os
import sys
from copy import copy
import math
import json
import hashlib
import pickle
import gzip
from collections import defaultdict
from pyxdameraulevenshtein import damerau_levenshtein_distance
from sympound.RedisClient import RedisClient

rc = RedisClient("localhost", 6379, "")

class SympoundRedis(object):
    """Redis Wrapper for sympound"""
    def __init__(self, redis_host, redis_port, redis_password):
        self.rc = RedisClient(redis_host, redis_port, redis_password)

class SuggestItem(object):
    def __init__(self, term="", distance = 0, count = 0):
        self.term = term
        self.distance = distance
        self.count = count

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(self, other.__class__):
            return self.term == other.term
        return False

    def __gt__(self, si2):
        """ a simple, default, comparison """
        if self.distance != si2.distance:
            return self.distance > si2.distance
        return self.count < si2.count

    def __str__(self):
        return self.term + ":" + str(self.count) + ":" + str(self.distance)


class Sympound(object):
    def __init__(self, maxDictionaryEditDistance=3, prefixLength=7, countThreshold=1):
        self.distance_func = damerau_levenshtein_distance
        self.max_dict_edit_distance = maxDictionaryEditDistance
        self.prefix_length = prefixLength
        self.count_threshold = countThreshold

        # false: assumes input string as single term, no compound splitting / decompounding
        # true:  supports compound splitting / decompounding with three cases:
        # 1. mistakenly inserted space into a correct word led to two incorrect terms
        # 2. mistakenly omitted space between two correct words led to one incorrect combined term
        # 3. multiple independent input terms with/without spelling errors
        self.edit_distance_max = 2
        self.verbose = 0  # //ALLWAYS use verbose = 0 if enableCompoundCheck = true!
        # 0: top suggestion
        # 1: all suggestions of smallest edit distance
        # 2: all suggestions <= editDistanceMax (slower, no early termination)
        # Dictionary that contains both the original words and the deletes derived from them. A term might be both word and delete from another word at the same time.
        # For space reduction a item might be either of type dictionaryItem or Int.
        # A dictionaryItem is used for word, word/delete, and delete with multiple suggestions. Int is used for deletes with a single suggestion (the majority of entries).
        # A Dictionary with fixed value type (int) requires less memory than a Dictionary with variable value type (object)
        # To support two types with a Dictionary with fixed type (int), positive number point to one list of type 1 (string), and negative numbers point to a secondary list of type 2 (dictionaryEntry)
        self.deletes = defaultdict(list)
        self.words = {}
        self.below_threshold_words = {}
        self.max_length = 0
        self.use_redis = True


    def delete_model(self, speller_id):
        """Delete all dict_entry for speller_id"""
        return rc.delete(speller_id)

    def create_dictionary_entry(self, speller_id, key, count):
        if (count <= 0):
            if self.count_threshold > 0:
                return False
            count = 0
        count_previous = -1

        condition = rc.exists_words(speller_id, key) if self.use_redis else key in self.words
        if self.count_threshold > 1 and key in self.below_threshold_words:
            count = count_previous+count if (sys.maxsize - count_previous > count) else sys.maxsize
            if count >= self.count_threshold:
                self.below_threshold_words.pop(key)
            else:
                self.below_threshold_words[key] = count
                return False
        # elif key in self.words:
        elif condition:
            count = count_previous+count if (sys.maxsize - count_previous > count) else sys.maxsize
            # self.words[key] = count
            if self.use_redis:
                rc.set_words(speller_id, key, count)
            else:
                self.words[key] = count
            return False
        elif count < self.count_threshold:
            self.below_threshold_words[key] = count
            return False
        # self.words[key] = count
        if self.use_redis:
            rc.set_words(speller_id, key, count)
        else:
            self.words[key] = count

        if self.use_redis:
            if len(key) > rc.get_max_length(speller_id):
                rc.set_max_length(speller_id, len(key))
        else:
            if len(key) > self.max_length:
                self.max_length = len(key)

        edits = self.edits_prefix(key)
        for delete in edits:
            deleteHash = self.get_string_hash(delete)
            # self.deletes[deleteHash].append(key)
            if self.use_redis:
                rc.append_deletes(speller_id, deleteHash, key)
            else:
                self.deletes[deleteHash].append(key)
        return True

    def get_string_hash(self, s):
        return hashlib.md5(s.encode('utf-8')).hexdigest()

    def save_pickle(self, filename, compressed=True):
        pickle_data = {"deletes": self.deletes, "words": self.words, "max_length": self.max_length}
        print(json.dumps(pickle_data, indent = 2))
        with (gzip.open if compressed else open)(filename, "wb") as f:
            pickle.dump(pickle_data, f)

    def load_pickle(self, filename, compressed=True):
        with (gzip.open if compressed else open)(filename, "rb") as f:
            pickle_data = pickle.load(f)
        self.deletes = pickle_data["deletes"]
        self.words = pickle_data["words"]
        self.max_length = pickle_data["max_length"]
        return True

    def delete_in_suggestion_prefix(self, delete, delete_len, suggestion, suggestion_len):
        if delete_len == 0:
            return True
        if self.prefix_length < suggestion_len:
            suggestion_len = self.prefix_length
        j = 0
        for c in delete:
            while j < suggestion_len and c != suggestion[j]:
                j += 1
            if j == suggestion_len:
                return False
        return True

    def load_dictionary(self, filepath=None, dict_tokens=None, term_index=0, count_index=1):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                tokens = line.split()
                if len(tokens) >= 2:
                    key = tokens[term_index]
                    count = int(tokens[count_index])
                    self.create_dictionary_entry("local_speller", key=key, count=count)
        self.below_threshold_words = {}
        return True

    def add_lowest_distance(self, item, suggestion, suggestion_int, delete):
        if self.verbose < 2 and len(item.suggestions) > 0 and (
                len(self.word_list[item.suggestions[0]]) - len(delete)) > (len(suggestion) - len(delete)):
            item.suggestions.clear()

        if self.verbose == 2 or len(item.suggestions) == 0 or (
                    len(self.word_list[item.suggestions[0]]) - len(delete) >= len(suggestion) - len(delete)):
            item.suggestions.append(suggestion_int)
        return item

    def edits_prefix(self, key):
        hashSet = []
        keylen = len(key)
        if keylen <= self.max_dict_edit_distance:
            hashSet.append("")
        if keylen > self.prefix_length:
            key = key[:self.prefix_length]
        hashSet.append(key)
        return self.edits(key, 0, hashSet)

    def edits(self, word, edit_distance, deletes):
        edit_distance += 1
        wordlen = len(word)
        if wordlen > 1:
            for index in range(0, wordlen):
                delete = word[:index] + word[index + 1:]
                if delete not in deletes:
                    deletes.append(delete)
                    if edit_distance < self.max_dict_edit_distance:
                        self.edits(delete, edit_distance, deletes)
        return deletes

    def lookup(self, speller_id, input_string, verbosity, edit_distance_max):
        if edit_distance_max > self.max_dict_edit_distance:
            return []
        input_len = len(input_string)

        if self.use_redis:
            local_max_length = rc.get_max_length(speller_id)
        else:
            local_max_length = self.max_length

        if (input_len - edit_distance_max) > local_max_length:
            return []

        suggestions = [] # list of SuggestItems
        hashset1 = set()
        hashset2 = set()

        # if input_string in self.words:
        condition = None
        if self.use_redis:
            condition = rc.exists_words(speller_id, input_string)
        else:
            condition = input_string in self.words
        # print(condition)
        if condition:
            # suggestions.append(SuggestItem(input_string, 0, self.words[input_string]))
            old_word = None
            if self.use_redis:
                old_word = rc.get_words(speller_id, input_string)
            else:
                old_word = self.words[input_string]
            # print(old_word)
            suggestions.append(SuggestItem(input_string, 0, old_word))

        hashset2.add(input_string)

        edit_distance_max2 = edit_distance_max
        candidates_index = 0
        singleSuggestion = [""]
        candidates = [] # list of strings

        input_prefix_len = input_len
        if input_prefix_len > self.prefix_length:
            input_prefix_len = self.prefix_length
            candidates.append(input_string[:input_prefix_len])
        else:
            candidates.append(input_string)
        while candidates_index < len(candidates):
            candidate = candidates[candidates_index]
            candidates_index+=1
            candidate_len = len(candidate)
            lengthDiff = input_prefix_len - candidate_len

            if lengthDiff > edit_distance_max2:
                if verbosity == 2:
                    continue
                break
            candidateHash = self.get_string_hash(candidate)
            condition = None
            if self.use_redis:
                condition = rc.exists_deletes(speller_id, candidateHash)
            else:
                condition = candidateHash in self.deletes
            # print(condition)
            if condition:
                dict_suggestions = None
                if self.use_redis:
                    dict_suggestions = rc.get_deletes(speller_id, candidateHash)
                else:
                    dict_suggestions = self.deletes[candidateHash]
                # print(dict_suggestions)

                for suggestion in dict_suggestions:
                    if suggestion == input_string:
                        continue
                    suggestion_len = len(suggestion)
                    if (abs(suggestion_len - input_len) > edit_distance_max2 or
                        suggestion_len < candidate_len or
                        (suggestion_len == candidate_len and suggestion != candidate)):
                        continue
                    sugg_prefix_len = min(suggestion_len, self.prefix_length)
                    if sugg_prefix_len > input_prefix_len and (sugg_prefix_len - candidate_len) > edit_distance_max2:
                        continue
                    distance = 0
                    if candidate_len == 0:
                        distance = min(input_len, suggestion_len)
                        if distance > edit_distance_max2:
                            continue
                        if suggestion in hashset2:
                            continue
                        hashset2.add(suggestion)
                    elif suggestion_len == 1:
                        if input_string.find(suggestion[0]) < 0:
                            distance = input_len
                        else:
                            distance = input_len -1
                        if distance > edit_distance_max2:
                            continue
                        if suggestion in hashset2:
                            continue
                        hashset2.add(suggestion)
                    else:
                        len_min = min(input_len, suggestion_len) - self.prefix_length
                        if ((self.prefix_length - edit_distance_max == candidate_len and
                             len_min > 1 and input_string[input_len+1-len_min:] != suggestion[suggestion_len+1-len_min:]) or
                            (len_min > 0 and input_string[input_len-len_min] != suggestion[suggestion_len-len_min] and
                            (input_string[input_len-len_min-1] != suggestion[suggestion_len-len_min] or input_string[input_len-len_min] != suggestion[suggestion_len-len_min-1]))):
                            continue
                        else:
                            if verbosity < 2 and not self.delete_in_suggestion_prefix(candidate, candidate_len, suggestion, suggestion_len) or suggestion in hashset2:
                                continue
                            if suggestion not in hashset2:
                                hashset2.add(suggestion)
                            distance = self.distance_func(input_string, suggestion)
                            if distance < 0:
                                continue
                    if distance <= edit_distance_max2:
                        # suggestion_count = self.words[suggestion]
                        suggestion_count = None
                        if self.use_redis:
                            suggestion_count = rc.get_words(speller_id, suggestion)
                        else:
                            suggestion_count = self.words[suggestion]
                        # print(suggestion_count)
                        si = SuggestItem(suggestion, distance, suggestion_count)
                        if len(suggestions) > 0:
                            if verbosity == 1:
                                if distance < edit_distance_max2:
                                    suggestions = []
                                break
                            elif verbosity == 0:
                                if distance < edit_distance_max2 or suggestion_count > suggestions[0].count:
                                    edit_distance_max2 = distance
                                    suggestions[0] = si
                                continue
                        if verbosity < 2:
                            edit_distance_max2 = distance
                        suggestions.append(si)

            if lengthDiff < edit_distance_max and candidate_len <= self.prefix_length:
                if verbosity < 2 and lengthDiff > edit_distance_max2:
                    continue
                for index in range(0, candidate_len):
                    delete = candidate[:index] + candidate[index + 1:]
                    if delete not in hashset1:
                        candidates.append(delete)
                    else:
                        hashset1.add(delete)
        if len(suggestions) > 1:
            suggestions = sorted(suggestions)
        return suggestions

    def set_edit_distance(self, input_string):
        """Set edit distance"""

        if len(input_string) <=3:
            return 0
        if len(input_string) in [4, 5]:
            return 1
        if len(input_string) in [6, 7]:
            return 2
        return 3

    def lookup_compound(self, speller_id, input_string):
        term_list_1 = input_string.split()
        suggestions = []
        suggestion_parts = []

        last_combi = False

        for i in range(len(term_list_1)):

            edit_distance_max = self.set_edit_distance(term_list_1[i])

            suggestions_previous_term = [copy(suggestion) for suggestion in suggestions]
            suggestions = self.lookup(speller_id, term_list_1[i], 0, edit_distance_max)
            if i > 0 and not last_combi:
                suggestions_combi = self.lookup(speller_id, term_list_1[i-1] + term_list_1[i], 0, edit_distance_max)
                if len(suggestions_combi) > 0:
                    best1 = suggestion_parts[-1]
                    best2 = None
                    if len(suggestions) > 0:
                        best2 = suggestions[0]
                    else:
                        best2 = SuggestItem(term_list_1[i], edit_distance_max+1, 0)
                    distance1 = self.distance_func(term_list_1[i - 1] + " " + term_list_1[i], best1.term + " " + best2.term)
                    if distance1 > 0 and suggestions_combi[0].distance + 1 < distance1:
                        suggestions_combi[0].distance += 1
                        suggestion_parts[-1] = suggestions_combi[0]
                        last_combi = True
                        break
            last_combi = False

            if len(suggestions) > 0 and (suggestions[0].distance == 0 or len(term_list_1[i]) == 1):
                suggestion_parts.append(suggestions[0])
            else:
                suggestions_split = []
                if len(suggestions) > 0:  # 473
                    suggestions_split.append(suggestions[0])
                if len(term_list_1[i]) > 1:
                    for j in range(1, len(term_list_1[i])):
                        part1 = term_list_1[i][0:j]
                        part2 = term_list_1[i][j:]
                        suggestion_split = SuggestItem()
                        suggestions1 = self.lookup(speller_id, part1, 0, edit_distance_max)
                        if len(suggestions1) > 0:
                            if len(suggestions) > 0 and suggestions[0].term == suggestions1[0].term:
                                break
                            suggestions2 = self.lookup(speller_id, part2, 0, edit_distance_max)
                            if len(suggestions2) > 0:
                                # if split correction1 == einzelwort correction
                                if len(suggestions) > 0 and suggestions[0].term == suggestions2[0].term:
                                    break
                                suggestion_split.term = suggestions1[0].term + " " + suggestions2[0].term
                                distance2 = self.distance_func(term_list_1[i], suggestions1[0].term + " " + suggestions2[0].term)
                                if distance2 < 0:
                                    distance2 = edit_distance_max+1
                                suggestion_split.distance = distance2
                                suggestion_split.count = min(suggestions1[0].count, suggestions2[0].count)
                                suggestions_split.append(suggestion_split)
                                if suggestion_split.distance == 1:
                                    break
                    if len(suggestions_split) > 0:
                        suggestions_split = sorted(suggestions_split, key=lambda x: 2 * x.distance - x.count, reverse=False)
                        suggestion_parts.append(suggestions_split[0])
                    else:
                        si = SuggestItem(term_list_1[i], edit_distance_max+1, 0)
                        suggestion_parts.append(si)
                else:
                    si = SuggestItem(term_list_1[i], edit_distance_max+1, 0)
                    suggestion_parts.append(si)
        suggestion = SuggestItem()
        suggestion.count = math.inf
        s = ""
        for si in suggestion_parts:
            s += si.term + " "
            suggestion.count = min(si.count, suggestion.count)
        suggestion.term = s.strip()
        suggestion.distance = self.distance_func(suggestion.term, input_string)
        return suggestion.term
