import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix

class preprocessing():

    def clean(self,x):
        x = str(x).replace(" ","").lower().strip()
        return x

    def preprocess_address(self,df):
        df[['bill_to_street', 'bill_to_postal_code','first_name']] = df[['bill_to_street', 'bill_to_postal_code','first_name']].applymap(self.clean)
        df['full_string'] = df['bill_to_street'] + "$" + df['bill_to_postal_code'] + "$" + df['first_name']
        full_string = list(df['full_string'])
        return df,full_string

    def preprocess_email_base1(self,df):
        df[['email_address', 'ship_to_postal_code','first_name', 'last_name']] = df[['email_address', 'ship_to_postal_code','first_name', 'last_name']].applymap(self.clean)
        df['email_address1'] = df['email_address'].apply(lambda x: x.split('@')[0])
        df['full_string'] = df['email_address1'] + "$" + df['ship_to_postal_code'] + "$" + df['first_name'].fillna('') + "$" + df['last_name'].fillna('')
        full_string = list(df['full_string'])
        return df,full_string

    def preprocess_email_base2(self,df):
        df[['email_address','bill_to_postal_code','bill_to_street','first_name']] = df[['email_address', 'bill_to_postal_code','bill_to_street','first_name']].applymap(self.clean)
        df['email_address1'] = df['email_address'].apply(lambda x: x.split('@')[0])
        df['full_string'] = df['email_address1'] + "$" + df['bill_to_postal_code'] + "$" + df['bill_to_street']+ "$" + df['first_name'].fillna('')
        full_string = list(df['full_string'])
        return df,full_string

    def preprocess_payment(self,df):
        df[['last_4_digits', 'bill_to_postal_code','ship_to_postal_code','bill_to_street','first_name']] = df[['last_4_digits', 'bill_to_postal_code','ship_to_postal_code','bill_to_street','first_name']].applymap(self.clean)
        df['full_string'] = df['last_4_digits'] + '$' + df['bill_to_postal_code'] + "$" + df['ship_to_postal_code'] + "$" + df['bill_to_street'] + "$" + df['first_name']
        full_string = list(df['full_string'])
        return df,full_string
    
    def preprocess_phone_base1(self,df):
        df[['phone','first_name']] = df[['phone','first_name']].applymap(self.clean)
        df['full_string'] = df['phone']+'$'+ df['first_name']
        full_string = list(df['full_string'])
        return df,full_string

    def preprocess_phone_base2(self,df):
        df[['phone','first_name','last_name']] = df[['phone','first_name','last_name']].applymap(self.clean)
        df['full_string'] = df['phone']+'$'+ df['first_name'] + '$' + df['last_name']
        full_string = list(df['full_string'])
        return df,full_string
