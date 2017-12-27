#!/usr/bin/env python
#coding: utf-8
import sys

with open('word_filte_dict/final','rt') as f:
    eng_brands = set([ line.strip() for line in f.readlines() ] )

with open('word_filte_dict/filter_word','rt') as f:
    common_cn_words = set([line.strip() for line in f.readlines() ])

with open('word_filte_dict/mess_word','rt') as f:
    mess_words = set([line.strip() for line in f.readlines() ])

for line in sys.stdin:
    word_cnt = line.strip().split()
    if len(word_cnt) != 2: continue
    word, cnt = word_cnt
    if len(word.decode('utf-8')) == 1: continue
    if int(cnt) <= 100 : continue
    c = word[0]
    if c >= '0' and c <= '9' : continue
    if c >= 'a' and c <= 'z' :
        if word not in eng_brands:
            continue
    if word in common_cn_words:continue
    if word in mess_words:continue
    print line.strip()

