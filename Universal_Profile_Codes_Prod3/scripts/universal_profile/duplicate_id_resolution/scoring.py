import sys
import json
import pandas as pd
from google.cloud import bigquery as bgq
from scipy.sparse import csr_matrix
from scipy.sparse import isspmatrix_csr
from sparse_dot_topn import sparse_dot_topn as ct
from sparse_dot_topn import sparse_dot_topn_threaded as ct_thread
from preprocessing import preprocessing
from sklearn.feature_extraction.text import TfidfVectorizer
from postprocess import postprocess
# from .user_input import *
from multiprocessing import Pool
from tqdm import tqdm
import numpy as np


def awesome_cossim_topn(A, B, ntop, lower_bound=0.4, use_threads=True, n_jobs=8, return_best_ntop=False, test_nnz_max=-1):
    """
    Arguments:
        A(csr_matrix) : TF-IDF matrix
        B(csr_matrix) : TF-IDF matrix transposed
        ntop(int) : top-n similarity score
    Returns:
        Compressed sparse row matrix : Matrix containing the top n most similar strings
    """
    def try_malloc(sz: int, idx_dtype, data_dtype) -> bool:
        try:
            ind_arr = np.empty(sz, dtype=idx_dtype)
            dat_arr = np.empty(sz, dtype=data_dtype)
            del ind_arr, dat_arr
            return True
        except MemoryError:
            return False
        
    if not isspmatrix_csr(A):
        A = A.tocsr()
    if not isspmatrix_csr(B):
        B = B.tocsr()

    dtype = A.dtype
    assert B.dtype == dtype
    lower_bound = dtype.type(lower_bound)  # Casting this scalar to the same type

    M, K1 = A.shape
    K2, N = B.shape

    if K1 != K2:
        err_str = 'A matrix multiplication will be operated. A.shape[1] must be equal to B.shape[0]!'
        raise ValueError(err_str)

    idx_dtype = np.int32

    nnz_max = M*ntop

    # basic check. if A or B are all zeros matrix, return all zero matrix directly
    if len(A.indices) == 0 or len(B.indices) == 0:
        indptr = np.zeros(M + 1, dtype=idx_dtype)
        indices = np.zeros(nnz_max, dtype=idx_dtype)
        data = np.zeros(nnz_max, dtype=A.dtype)
        output = csr_matrix((data, indices, indptr), shape=(M, N))
        if return_best_ntop:
            return output, 0
        else:
            return output

    indptr = np.empty(M + 1, dtype=idx_dtype)

    # reduce nnz_max if too large to fit in available memory:
    nnz_max = 16*nnz_max
    while (not try_malloc(nnz_max, idx_dtype, A.dtype)):
        nnz_max = nnz_max//2

    # take a chance on high matrix-sparsity and reduce further:
    nnz_max = max(M, nnz_max//16)
    
    # this line is only for testing purposes, designed to enable the user to 
    # force C/C++ to reallocate memory during the matrix multiplication
    nnz_max = test_nnz_max if test_nnz_max > 0 else nnz_max
    
    # filled matrices from here on
    indices = np.empty(nnz_max, dtype=idx_dtype)
    data = np.empty(nnz_max, dtype=A.dtype)

    best_ntop_arr = np.full(1, 0, dtype=idx_dtype)

    if not use_threads:

        alt_indices, alt_data = ct.sparse_dot_topn_extd(
            M, N, np.asarray(A.indptr, dtype=idx_dtype),
            np.asarray(A.indices, dtype=idx_dtype),
            A.data,
            np.asarray(B.indptr, dtype=idx_dtype),
            np.asarray(B.indices, dtype=idx_dtype),
            B.data,
            ntop,
            lower_bound,
            indptr, indices, data, best_ntop_arr
        )

    else:
        if n_jobs < 1:
            err_str = 'Whenever you select the multi-thread mode, n_job must be greater than or equal to 1!'
            raise ValueError(err_str)

        alt_indices, alt_data = ct_thread.sparse_dot_topn_extd_threaded(
            M, N, np.asarray(A.indptr, dtype=idx_dtype),
            np.asarray(A.indices, dtype=idx_dtype),
            A.data,
            np.asarray(B.indptr, dtype=idx_dtype),
            np.asarray(B.indices, dtype=idx_dtype),
            B.data,
            ntop,
            lower_bound,
            indptr, indices, data, best_ntop_arr, n_jobs
        )

    if alt_indices is not None:
        indices = alt_indices
        data = alt_data

    # prepare and return the output:
    output = csr_matrix((data, indices, indptr), shape=(M, N))
    if return_best_ntop:
        return output, best_ntop_arr[0]
    else:
        return output

# Loading the params file
if __name__ == "__main__":
    params = json.load(open("id_resolution.json"))

print("scoring code is started...")
data = ['address','email_base1','email_base2', 'payment', 'phone_base1', 'phone_base2']

# Reading case-wise dataframes in a dictionary with the case name as the key 
df_dict = {i : pd.read_csv(params[i]) for i in data}

# Calling the preprocess class a and applying custom preprocessing funtions for each of the PII cases
preprocess = preprocessing()
ad = {}
full_string = {}
for name, df in df_dict.items():
    ad[(name)],full_string[(name)] = getattr(preprocess, 'preprocess_%s' % name)(df)    

# Constructing vectorizer for building the TF-IDF matrix
vectorizer = TfidfVectorizer("char", ngram_range=(1, 4), sublinear_tf=True)


# Applying Tfidf Vectorizer on each of the full sting of respective PII cases
tfidf = {}

for name, df in full_string.items():
    print(name)
    tfidf[(name)] = vectorizer.fit_transform(df)    


# Applying awesome_cossim_topn function on each of the matrix of respective PII cases
matches = {}

for name, mat in tfidf.items():
    print(name)
    matches[(name)] = awesome_cossim_topn(mat, mat.transpose(), 5) 


def get_matches_df(sparse_matrix, name_vector):

    """
    Arguments:
        sparse_matrix(csr_matrix) : Matrix containing the top n most similar strings
        name_vector(pd.series) : Series containg combined string of PII cases
    Returns:
        matches_df : Dataframe with the top n most similar strings
    """

    non_zeros = sparse_matrix.nonzero()
    
    sparserows = non_zeros[0]
    sparsecols = non_zeros[1]
    
    top = sparsecols.size
    
    if top:
        nr_matches = top
    else:
        nr_matches = sparsecols.size
    
    left_side = np.empty([nr_matches], dtype=object)
    right_side = np.empty([nr_matches], dtype=object)
    similairity = np.zeros(nr_matches)
    
    for index in range(0, nr_matches):
        left_side[index] = name_vector[sparserows[index]]
        right_side[index] = name_vector[sparsecols[index]]
        similairity[index] = sparse_matrix.data[index]
    
    return pd.DataFrame({'left_side': left_side,
                          'right_side': right_side,
                           'similarity': similairity})


#Creating a dictionary of matches and full-strings of the same PII cases in the key
result = {}
for key in (matches.keys() | full_string.keys()):
    if key in matches: result.setdefault(key, []).append(matches[key])
    if key in full_string: result.setdefault(key, []).append(full_string[key])


# Applying get_matches_df function on each of the matrix of respective PII cases
matched_dd = {}
for name, val in result.items():
    print(name)
    value_list = result[name]
    matched_dd[(name)] = get_matches_df(value_list[0],value_list[1])
print("We are at 215 line...")

# Calling the postprocess class and applying postprocessing functions on each of the dataframes of respective PII cases
postprocess = postprocess()
names = {}
final_df = {}
for name, df in matched_dd.items():
    names[(name)],final_df[(name)] = getattr(postprocess, 'postprocess_%s' % name)(df)    


# Combining names, ad and final_df for same PII cases with case name as key
result1 = {}
for key in (names.keys() | ad.keys() | final_df.keys()):
    if key in names: result1.setdefault(key, []).append(names[key])
    if key in ad: result1.setdefault(key, []).append(ad[key])
    if key in final_df: result1.setdefault(key, []).append(final_df[key])

def tag_bnid(left,right,i):
    """
    Function to match the string from matches_df and AD to get bnid and other PII columns
    Arguments:
        left : Left side column of matches_df 
        right : right side column of matches_df 
    Returns:
        sub_df : Dataframe with matches at row level and flag to link those matches
    """    
    ad_l = sub_df[sub_df['full_string']==left]
    ad_r = sub_df[sub_df['full_string']==right]
    
    sub_ad = pd.concat([ad_l,ad_r])
    sub_ad['dup_flag'] = i
    
    return sub_ad


print("We are at 250 line...")

def mult(function,comb_list,results):
    """
    Function to apply multiprocessing on tag_bnid and tag_bnid2 functions
    Arguments:
        function : name of the function 
        comb_list : list of inputs for the tag_bnid and tag_bnid2 functions
        results : empty list in which results of this function will be concatenated 
    Returns:
        dup_df : Dataframe with results from repective function
    """    
    comb_list = comb_list
    results = []
    # ,tqdm(comb_list,total=len(comb_list))
    
    with Pool(64) as spool:
        for d in spool.starmap(function, comb_list):
            results.append(d)
            pass
    spool.close()
    spool.join()
    dup_df = pd.concat(results)
    return dup_df


# Apply tag_bnid function using multiprocessing mult function on each PII case
dup = {}

for name, val in result1.items():
    print(name)
    value_list = result1[name]
    sub_df = value_list[1][value_list[1]["full_string"].isin(value_list[0])]
    comb_list = [(i,j,k) for i,j,k in zip(value_list[2]['left_side'],value_list[2]['right_side'],np.arange(1,value_list[2].shape[0]+1))]
    rs = []
    dup[(name)] = mult(tag_bnid,comb_list,rs)

print("We are at 288 line...")
def tag_bnid_2(bnid_list,i):
    """
    Function to convert the output in the final format as in the current bnid process
    Arguments:
        bnid_list (list) : list of bnids 
        i (int) : Creating a flag variable to link bnids 
    Returns:
        final (dataframe) : Dataframe with results in the final format, as in current bnid process
    """    
    a = list(dup_df[dup_df['bnid'] == bnid_list]['dup_flag'].unique())
    b = dup_df[dup_df['dup_flag'].isin(a)]
    b = b.drop(columns=['dup_flag'])
    b = b.drop_duplicates()
    b['new_flag'] = np.where(b['first_event_date'] == b['first_event_date'].min(),'primary','secondary')
    b['dup_flag_new'] = i
    primary = b[b['new_flag'] == 'primary'][['bnid','first_event_date','dup_flag_new']]
    secondary = b[b['new_flag'] == 'secondary'][['bnid','first_event_date','dup_flag_new']]
    primary.columns = ['primary_bnid','primary_first_event_date','dup_flag_new']
    secondary.columns = ['secondary_bnid','secondary_first_event_date','dup_flag_new']
    final = primary.merge(secondary,left_on='dup_flag_new', right_on='dup_flag_new')
    final = final[['primary_bnid', 'primary_first_event_date','secondary_bnid', 'secondary_first_event_date']]
    final = final.drop_duplicates()
    return final

#Combining results from all PII cases and creating a final dataframe
final = {}
for name, val in dup.items():
    print(name)
    dup_df = dup[name]
    d = dup_df[['bnid']]
    d = d.groupby(["bnid"])["bnid"].count().reset_index(name="count")
    bnid_list1 = list(d[d['count']>1]['bnid'])
    bnid_list2 = list(d[d['count']==1]['bnid'])
    comb_list_1 = [(i,j) for i,j in zip(bnid_list1,np.arange(1,len(bnid_list1)))]
    comb_list_2 = [(i,j) for i,j in zip(bnid_list2,np.arange(1,len(bnid_list2)))]
    rt1 = []
    dup1 = mult(tag_bnid_2,comb_list_1,rt1)
    rt2 = []
    dup2 = mult(tag_bnid_2,comb_list_2,rt2)
    final_output = pd.concat([dup1, dup2])
    final_output = final_output.drop_duplicates()
    final[(name)] = final_output

predicted_df = pd.concat(final, axis=0).reset_index(level=0).rename({'level_0':'Method'}, axis=1)
predicted_df = predicted_df[['primary_bnid','primary_first_event_date','secondary_bnid','secondary_first_event_date','Method']]
predicted_df.to_csv(params['output_path'] + "id_resolution.csv",index=False)

print("Its Done...")