import pandas as pd
import numpy as np

class PreProcess:
    """ 
    Super class for all functions - Normalize, Calc_Values, topsis_score
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe
    
    def Normalize(self, nCol, weights):
        """
        Description: This function gives us dataframe as output with normalized column values.
        Arguments for this function are dataframe, number of columns, weights of each column.
        """
        for i in range(1, nCol):
            temp = 0
            # Calculating Root of Sum of squares of a particular column
            for j in range(len(self.dataframe)):
                temp = temp + self.dataframe.iloc[j, i]**2
            temp = temp**0.5
            # Weighted Normalizing a element
            for j in range(len(self.dataframe)):
                self.dataframe.iat[j, i] = (self.dataframe.iloc[j, i] / temp)*weights[i-1]
        return self.dataframe
    
    def Calc_Values(self, nCol, impact):
        """
        Description: This function gives us ideal best and ideal worst values as output. We will find out ideal best and ideal worst
        values by seeing the impact("+" or "-"). If "+" impact then ideal best for that column is the maximum value in that column
        and ideal worst is the minimum value in that column and viceversa for "-" impact.  
        Arguments for this function are dataframe, number of columns, type of impact 
        """
        p_sln = (self.dataframe.max().values)[1:]
        n_sln = (self.dataframe.min().values)[1:]
        for i in range(1, nCol):
            if impact[i-1] == '-':
                p_sln[i-1], n_sln[i-1] = n_sln[i-1], p_sln[i-1]
        return p_sln, n_sln
    


