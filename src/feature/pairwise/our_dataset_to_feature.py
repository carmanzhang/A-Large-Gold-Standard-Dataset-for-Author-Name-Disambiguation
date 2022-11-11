import os
from multiprocessing import Pool

import sys

import joblib
from scipy import stats

sys.path.append('../../')

import pandas as pd
from gensim.models import Doc2Vec
from mytookit.data_reader import DBReader
from scipy.spatial.distance import cosine
from sklearn.feature_extraction.text import TfidfVectorizer

from eutilities.string_utils import jaccard_similarity, extract_word_list, ngram_sequence, \
    convert_unicode_to_ascii
from myconfig import cached_dir

df = DBReader.tcp_model_cached_read("XXXX",
                                    sql="select * from and_ds.our_and_dataset_pairwise_gold_standard;",
                                    cached=False)
print('df.shape', df.shape)

# ['fullname' 'pid1' 'ao1' 'pid2' 'ao2' 'same_author' 'authors1'
#  'paper_title1' 'venue1' 'pub_year1' 'authors2' 'paper_title2' 'venue2'
#  'pub_year2', 'train1_test0_val2']
columns = df.columns.values
print(len(columns), columns)
h, w = df.shape


def concat_title_abstract(row):
    return ' '.join([str(n) for n in row.values]).lower()


documents = list(
    df[['paper_title1', 'abstract1']].apply(concat_title_abstract, axis=1).values) + list(
    df[['paper_title2', 'abstract2']].apply(concat_title_abstract, axis=1).values)
vectorizer = TfidfVectorizer()  # tokenizer=normalize, stop_words='english'
print('fit tfidf model')
vectorizer = vectorizer.fit(documents)


def cosine_sim(text1, text2):
    tfidf = vectorizer.transform([text1, text2])
    return ((tfidf * tfidf.T).A)[0, 1]


# load doc2vec model
model = Doc2Vec.load(os.path.join(cached_dir, 'doc2vec_model'))
print('load doc2vec model')


def extract_pairwise_feature(pairwise_citation):
    task_id, (fullname, pid1, ao1, pid2, ao2,
              coauthor1, aid1, author_names1, aff_arr1, aff_id_arr1, paper_title1, abstract1, venue1, pub_year1,
              coauthor2, aid2, author_names2, aff_arr2, aff_id_arr2, paper_title2, abstract2, venue2, pub_year2,
              same_author, train1_test0_val2) = pairwise_citation

    try:
        if task_id % 10000 == 0:
            print(task_id * 100.0 / h)

        author_names1, author_names2 = author_names1.lower(), author_names2.lower()

        # if author_names1 != convert_unicode_to_ascii(author_names1):
        #     print(author_names1, convert_unicode_to_ascii(author_names1))

        # name similarity
        name_similarity = jaccard_similarity(ngram_sequence(convert_unicode_to_ascii(author_names1)),
                                             ngram_sequence(convert_unicode_to_ascii(author_names2)))

        same_biblio_aid = 1 if aid1 == aid2 else 0
        pub_year_diff = abs(pub_year1 - pub_year2) if pub_year1 > 0 and pub_year2 > 0 else -1

        try:
            content1, content2 = (paper_title1 + ' ' + str(abstract1)).lower(), (paper_title2 + ' ' + str(abstract2)).lower()
        except Exception as e:
            print(e)
            content1, content2 = paper_title1, paper_title2

        word_list1 = extract_word_list(content1)
        word_list2 = extract_word_list(content2)
        paper_title_abstract_similarity = jaccard_similarity(
            word_list1,
            word_list2,
            remove_stop_word=True)

        # do2vec similarity
        content_cosin_sim = 0
        try:
            v1 = model.infer_vector(word_list1, steps=12, alpha=0.025)
            v2 = model.infer_vector(word_list2, steps=12, alpha=0.025)
            # Compute the Cosine distance between 1-D arrays.
            # distance cosine([1, 2],[3,4]) = 1 - (1*3+2*4)/(sqrt(1*1+2*2) * sqrt(3*3+4*4))
            content_cosin_sim = 1 - cosine(v1, v2)
        except Exception as e:
            print(e)

        # tfidf similarity
        tfidf_cosin_sim = 0
        try:
            tfidf_cosin_sim = cosine_sim(content1, content2)
        except Exception as e:
            print(e)

        venue_similarity = jaccard_similarity(extract_word_list(str(venue1).lower()),
                                              extract_word_list(str(venue2).lower()))

        aff_similarity = jaccard_similarity(extract_word_list(' '.join(str(aff_arr1).split('|')).lower()),
                                            extract_word_list(' '.join(str(aff_arr2).split('|')).lower()))

        feature_item = [fullname, pid1, ao1, pid2, ao2, same_author, train1_test0_val2,
                        name_similarity, same_biblio_aid, pub_year_diff,
                        paper_title_abstract_similarity,
                        content_cosin_sim, tfidf_cosin_sim,
                        venue_similarity,
                        aff_similarity,
                        content1,
                        content2]

        return feature_item
    except Exception as e:
        print(e)
        return [fullname, pid1, ao1, pid2, ao2, same_author, train1_test0_val2,
                0, 0, 0, 0, 0, 0, 0, 0, "", ""]


task_pools = [(i, row) for i, row in df.iterrows()]

with Pool(processes=14) as pool:
    features = pool.map(extract_pairwise_feature, task_pools)

joblib.dump(features, 'tmp.pkl')

pd.DataFrame(features,
             columns=['fullname', 'pid1', 'ao1', 'pid2', 'ao2',
                      'same_author', 'train1_test0_val2',
                      'name_similarity', 'same_biblio_aid', 'pub_year_diff',
                      'paper_title_abstract_similarity', 'content_cosin_similarity', 'tfidf_cosin_similarity',
                      'venue_similarity', 'aff_similarity', 'content1', 'content2']).to_csv(
    os.path.join(cached_dir, 'pairwise_and_dataset_feature_full.tsv'), sep='\t', index=False)
