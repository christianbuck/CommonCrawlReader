#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import codecs
import gzip
from collections import defaultdict
from operator import itemgetter
import langid
import re

magic_number = "df6fa1abb58549287111ba8d776733e9"
sentence_end_punctuation = set(list(u".!?¡¿)"))

def join_lines(document, lang, min_words=8):
    joined_lines = []
    for linenr, line in enumerate(document):
        if linenr >= len(document)-1:
            continue
        split_line = line.rstrip().split()
        if len(split_line) < min_words and not (joined_lines and joined_lines[-1] == linenr-1):
            continue
        last_word = split_line[-1]
        if last_word[-1] in sentence_end_punctuation:
            continue
        next_line = document[linenr+1]
        if len(next_line.strip().split()) < 5:
            # the next line should not be too short
            continue
        if lang == 'en' and next_line.strip()[0].isupper():
            # English sentences are not be continued with an uppercase character
            continue
        if not re.match(r'\s*\w', next_line, re.UNICODE):
            # next line should start with alphanumeric character ...
            continue
        if re.match(r'\s*\d', next_line, re.UNICODE):
            # ... but not with a number
            continue
        document[linenr] = "%s  " %line.strip() # two spaces to mark joining
        #sys.stderr.write("joining:\n%s + %s" %(line, next_line))
        joined_lines.append(linenr)
    return len(joined_lines)

def process_document(document, url, outfiles):
    n_joined = 0
    if not document:
        return None, 0, 0, n_joined
    lang, confidence = langid.classify("".join(document))
    if confidence >= args.threshold:
        if not lang in outfiles:
            outfiles[lang] = codecs.getwriter('utf-8')(
                gzip.open("%s_%s.gz" %(args.prefix, lang), 'w'))
        outfiles[lang].write("%s %f %s\n" %(magic_number, confidence, url))
        n_joined += join_lines(document, lang)
        map(outfiles[lang].write, document)
    else:
        #sys.stderr.write("dropping %s lines from %s\n" %(len(document), url))
        return None, 0, len(document),  n_joined
    return lang, len(document), 0, n_joined


if __name__ == "__main__":
    #doc = ['Berlin (ots) - Carlsberg und HERTHA BSC haben ihre seit 2008 bestehende Partnerschaft auf die Saison 2012/2013 ausgedehnt. Der ursprünglich bis Juni 2012 angelegte Fünf-Jahres-Vertrag von ',
    #       'Carlsberg als "Exklusiv Partner von HERTHA BSC" wurde frühzeitig um ein Jahr verlängert. "Carlsberg hat großes Interesse an einer langfristigen Beziehung zum Hauptstadtverein. Wir wollen mit der Vertragsverlängerung als treuer Partner ein Zeichen setzen, dass wir auch in schwierigen Zeiten zur Hertha stehen", so Hermann Crux, Geschäftsführer der Carlsberg Bier GmbH. "Wir freuen uns sehr, dass Carlsberg auch in der Zweiten Liga unser Partner bleibt und darüber hinaus zum jetzigen Zeitpunkt den Vertrag verlängert hat", sagt HERTHA-Geschäftsführer Ingo Schiller.']
    #print join_lines(doc, 'de')
    #print doc
    #sys.exit()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('prefix', help="output prefix")
    parser.add_argument('-threshold', type=float, default=0.9999,
                        help="minimum confidence of langid")
    args = parser.parse_args(sys.argv[1:])

    document = []
    outfiles = {}
    url = None
    total_written = defaultdict(int)
    total_joined = 0
    total_dropped = 0

    for linenr, line in enumerate(sys.stdin):
        line = line.decode('utf-8')
        #if not line.strip():
        #    continue
        if line.startswith(magic_number):
            if url != None: # not first document
                assert url
                # assert document
                lang, written, dropped, joined = process_document(document, url, outfiles)
                total_dropped += dropped
                if lang != None:
                    total_written[lang] += written
                total_joined += joined
            url = line.split()[1].strip()
            document = []
        else:
            document.append(line)

    lang, written, dropped, joined = process_document(document, url, outfiles)
    total_dropped += dropped
    if lang != None:
        total_written[lang] += written
    total_joined += joined

    for f in outfiles.values():
        f.close()

    # report
    sys.stderr.write("--- Language Stats: ---\n")
    for lang, n_written in sorted(total_written.iteritems(),
                                  key=itemgetter(1), reverse=True):
        sys.stderr.write("%s\t%9d lines\n" %(lang, n_written))
    total_lines_written = sum(total_written.values())
    total_lines = total_lines_written + total_dropped
    sys.stderr.write("\nTOTAL WRITTEN: %10d lines = %3.2f%%\n"
                     %(total_lines_written,
                       100. * total_lines_written / total_lines))
    sys.stderr.write("      DROPPED: %10d lines = %3.2f%%\n"
                     %(total_dropped,
                       100. * total_dropped / total_lines))
    sys.stderr.write(" LINES JOINED: %10d lines = %3.2f%%\n\n"
                     %(total_joined, 100. * total_joined / total_lines))

