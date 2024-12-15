import datetime
from datetime import *
import time
import os
import pandas as pd
import numpy as np
import re
import time
import joblib
import pickle
from datetime import datetime, date


class PreProcess:
    """
    Super class for all functions - date_conversion, transformation_AD, modelling_AD_creation
    """

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def date_conversion(self, date_column):
        """Description: this function converts the date variables into datetime datatype
               Args:
                   dataframe (dataframe): Data For Aggregation
                   date_column: column to be converted into datetime
            """
        self.dataframe[date_column] = pd.to_datetime(
            self.dataframe[date_column], format="%Y%m%d", errors="ignore"
        )
        self.dataframe.index = self.dataframe[date_column]
        return self.dataframe

    def transformation_AD(self, dataframe, variable_type_list, level):
        """Description: this function converts the date variables into datetime datatype
               Args:
                   dataframe (dataframe): Data For Aggregation
                   variable_type_list: common variable_list
                   level: level of the data
        """
        columns_for_melting = [
            "browse_count_wedding",
            "browse_count_dj",
            "browse_count_oj",
            "add_to_basket_dj",
            "add_to_basket_wedding",
            "add_to_basket_oj",
            "dwell_time_seconds_oj",
            "dwell_time_seconds_dj",
            "dwell_time_seconds_wedding",
            "product_array_all_dj",
            "product_array_all_oj",
            "product_array_all_wedding",
            "catalog_page_view_wedding",
            "catalog_page_view_oj",
            "catalog_page_view_dj",
            "detail_page_view_wedding",
            "detail_page_view_oj",
            "detail_page_view_dj",
            "product_Browse_count_distinct_wedding",
            "product_Browse_count_distinct_dj",
            "product_Browse_count_distinct_oj",
            "product_browse_all_count_wedding",
            "product_browse_all_count_dj",
            "product_browse_all_count_oj",
            "number_of_desktop_visit_dj",
            "number_of_desktop_visit_oj",
            "number_of_desktop_visit_wedding",
            "number_of_mobile_visits_dj",
            "number_of_mobile_visits_oj",
            "number_of_mobile_visits_wedding",
            "search_count_number_wedding",
            "search_count_number_dj",
            "search_count_number_oj",
            "review_count_number_dj",
            "review_count_number_wedding",
            "review_count_number_oj",
            "education_page_count_num_wedding",
            "education_page_count_num_oj",
            "education_page_count_num_dj",
            "purchase_flag_oj",
            "purchase_flag_dj",
            "purchase_flag_wedding",
        ]
        category_list = ["wedding", "dj", "oj"]
        df_melted = pd.DataFrame()
        for category in category_list:
            df_category = dataframe.copy()
            df_1 = df_category[df_category["Short_Consideration_Category"] == category]
            df_journey_final_categorywise = df_1[level]
            for variable_type in variable_type_list:
                journey_list = [
                    i
                    for i in columns_for_melting
                    if variable_type in i and category in i
                ]
                other_journey_list = [
                    i
                    for i in columns_for_melting
                    if category not in i and variable_type in i
                ]
                df_orieng_journey = df_1[level + journey_list]

                df_other_journey = df_1[level + other_journey_list]
                if variable_type == "dwell_time_seconds_":
                    df_other_journey = df_other_journey.fillna(0)
                df_other_journey.columns = level + ["OJ_1", "OJ_2"]
                if variable_type == "product_array_all_":
                    df_other_journey[variable_type + "OJ"] = df_other_journey[
                        ["OJ_1", "OJ_2"]
                    ].agg(",".join, axis=1)
                else:
                    df_other_journey[variable_type + "OJ"] = (
                        df_other_journey["OJ_1"] + df_other_journey["OJ_2"]
                    )

                df_journey = df_orieng_journey.merge(
                    df_other_journey, on=level, how="left"
                )
                df_journey = df_journey[level + journey_list + [variable_type + "OJ"]]
                df_journey.columns = level + [
                    variable_type + "journey",
                    variable_type + "OJ",
                ]
                df_journey_final_categorywise = df_journey_final_categorywise.merge(
                    df_journey, on=level, how="left"
                )
            df_melted = df_melted.append(df_journey_final_categorywise)
        return df_melted.reset_index()

    def modelling_AD_creation(self, date, OD, PD):
        """Description: this function converts the data for final aggregation
               Args:
                   dataframe (dataframe): Data For Aggregation
                   variable_type_list: common variable_list
                   level: level of the data
            """
        time_start = time.time()
        current_date = np.datetime64(date)
        i = current_date - OD
        past_date = i - PD

        # guid keys for the previous day
        guid_keys = self.dataframe[
            (self.dataframe.index == i)
            & (self.dataframe["purchase_flag_oj"] == 0)
            & (self.dataframe["purchase_flag_dj"] == 0)
            & (self.dataframe["purchase_flag_wedding"] == 0)
        ]["guid_key"].unique()

        # PAST DATA
        df_past = self.dataframe[
            (self.dataframe.index <= i) & (self.dataframe.index > past_date)
        ]
        df_past = df_past[(df_past["guid_key"].isin(guid_keys))]
        df_past = df_past.sort_index()
        df_past[
            [
                "product_array_all_dj",
                "product_array_all_oj",
                "product_array_all_wedding",
            ]
        ] = df_past[
            [
                "product_array_all_dj",
                "product_array_all_oj",
                "product_array_all_wedding",
            ]
        ].astype(
            str
        )
        ######################################################################################
        df_past_1 = (
            df_past.groupby("guid_key")[
                "browse_count_wedding", "browse_count_dj", "browse_count_oj"
            ]
            .sum()
            .reset_index()
        )
        df_unpivoted = df_past_1.melt(
            id_vars="guid_key", var_name="Variable", value_name="Browsing_Activity"
        )
        df_unpivoted["Short_Consideration_Category"] = df_unpivoted[
            "Variable"
        ].str.split("_", expand=True)[2]
        df_unpivoted = df_unpivoted[(df_unpivoted["Browsing_Activity"] > 0)][
            ["guid_key", "Short_Consideration_Category", "Variable"]
        ]
        df_past = pd.merge(df_past, df_unpivoted, how="left", on="guid_key")
        df_past["GUID_PJ"] = (
            df_past["guid_key"].astype(str)
            + str("_")
            + df_past["Short_Consideration_Category"]
        )
        df_past_1 = df_past[
            [
                "GUID_PJ",
                "guid_key",
                "date_key",
                "Short_Consideration_Category",
                "catalog_page_view_wedding",
                "catalog_page_view_oj",
                "catalog_page_view_dj",
                "detail_page_view_wedding",
                "detail_page_view_oj",
                "detail_page_view_dj",
                "browse_count_wedding",
                "browse_count_dj",
                "browse_count_oj",
                "add_to_basket_dj",
                "add_to_basket_wedding",
                "add_to_basket_oj",
                "dwell_time_seconds_oj",
                "dwell_time_seconds_dj",
                "dwell_time_seconds_wedding",
                "product_array_all_dj",
                "product_array_all_oj",
                "product_array_all_wedding",
                "product_Browse_count_distinct_wedding",
                "product_Browse_count_distinct_dj",
                "product_Browse_count_distinct_oj",
                "product_browse_all_count_wedding",
                "product_browse_all_count_dj",
                "product_browse_all_count_oj",
                "number_of_desktop_visit_dj",
                "number_of_desktop_visit_oj",
                "number_of_desktop_visit_wedding",
                "number_of_mobile_visits_dj",
                "number_of_mobile_visits_oj",
                "number_of_mobile_visits_wedding",
                "search_count_number_wedding",
                "search_count_number_dj",
                "search_count_number_oj",
                "review_count_number_dj",
                "review_count_number_wedding",
                "review_count_number_oj",
                "education_page_count_num_wedding",
                "education_page_count_num_oj",
                "education_page_count_num_dj",
                "purchase_flag_oj",
                "purchase_flag_dj",
                "purchase_flag_wedding",
            ]
        ]
        level = ["GUID_PJ", "guid_key", "date_key", "Short_Consideration_Category"]

        variable_list = [
            "browse_count_",
            "catalog_page_view_",
            "detail_page_view_",
            "dwell_time_seconds_",
            "product_array_all_",
            "product_Browse_count_distinct_",
            "product_browse_all_count_",
            "number_of_desktop_visit_",
            "add_to_basket_",
            "number_of_mobile_visits_",
            "search_count_number_",
            "review_count_number_",
            "education_page_count_num_",
            "purchase_flag_",
        ]

        df_past_1 = df_past_1.sort_index()
        df_past_1 = self.transformation_AD(df_past_1, variable_list, level)

        df_2 = df_past[
            [
                "GUID_PJ",
                "dwell_time_seconds_site",
                "hit_per_day_site",
                "number_of_webchats",
                "page_visit_count_distinct",
                "showroom_page_visit",
            ]
        ]
        ###################################################################################################################################
        ###########################Independent Features################################
        df_ind = df_past_1[df_past_1["date_key"] == i][
            ["GUID_PJ", "guid_key", "date_key"]
        ]
        levels = ["journey", "OJ"]
        PD_07days = i - np.timedelta64(7, "D")
        PD_14days = i - np.timedelta64(14, "D")
        PD_28days = i - np.timedelta64(28, "D")
        dates_dict = {
            "from_first_Date": past_date,
            "PD_07days": PD_07days,
            "PD_14days": PD_14days,
            "PD_28days": PD_28days,
        }
        date_list = ["from_first_Date", "PD_07days", "PD_14days", "PD_28days"]

        ### Browse_count
        df_browse_count = df_past_1.copy()
        for date_type in date_list:
            df_browse_count_int = df_browse_count[
                df_browse_count["date_key"] >= dates_dict[date_type]
            ]
            df_browse_count_1 = df_browse_count_int.copy()
            df_browse_count_1 = (
                df_browse_count_1[
                    ["browse_count_journey", "browse_count_OJ", "GUID_PJ"]
                ]
                .groupby(["GUID_PJ"])
                .sum()
                .reset_index()
            )
            df_browse_count_1.columns = [
                "GUID_PJ",
                "browse_count_journey_" + date_type,
                "browse_count_OJ_" + date_type,
            ]
            df_ind = df_ind.merge(df_browse_count_1, how="left", on="GUID_PJ")

        # Catalog_detail_count
        catalog_detail_list = [
            "catalog_page_view_journey",
            "catalog_page_view_OJ",
            "detail_page_view_journey",
            "detail_page_view_OJ",
            "product_Browse_count_distinct_journey",
            "product_Browse_count_distinct_OJ",
            "product_browse_all_count_journey",
            "product_browse_all_count_OJ",
            "number_of_desktop_visit_journey",
            "number_of_desktop_visit_OJ",
            "number_of_mobile_visits_journey",
            "number_of_mobile_visits_OJ",
            "search_count_number_journey",
            "search_count_number_OJ",
            "review_count_number_journey",
            "review_count_number_OJ",
            "education_page_count_num_journey",
            "education_page_count_num_OJ",
        ]
        df_catalog_detail_count = df_past_1.copy()
        for date_type in date_list:
            df_catalog_detail_count_1 = df_catalog_detail_count[
                df_catalog_detail_count["date_key"] >= dates_dict[date_type]
            ]
            df_catalog_detail_count_int = df_catalog_detail_count_1.copy()
            df_catalog_detail_count_int_1 = (
                df_catalog_detail_count_int[catalog_detail_list + ["GUID_PJ"]]
                .groupby(["GUID_PJ"])
                .sum()
                .reset_index()
            )
            df_catalog_detail_count_int_1.columns = ["GUID_PJ"] + [
                str(j + "_" + date_type) for j in catalog_detail_list
            ]
            df_ind = df_ind.merge(
                df_catalog_detail_count_int_1, how="left", on="GUID_PJ"
            )

        # add_to_basket
        df_add_to_basket = df_past_1.copy()
        for date_type in date_list:
            df_add_to_basket_1 = df_add_to_basket[
                df_add_to_basket["date_key"] >= dates_dict[date_type]
            ]
            df_add_to_basket_count_int = df_add_to_basket_1.copy()
            df_add_to_basket_count_int_1 = (
                df_add_to_basket_count_int[
                    ["add_to_basket_journey", "add_to_basket_OJ", "GUID_PJ"]
                ]
                .groupby(["GUID_PJ"])
                .sum()
                .reset_index()
            )
            df_add_to_basket_count_int_1.columns = [
                "GUID_PJ",
                "add_to_basket_journey_" + date_type,
                "add_to_basket_OJ_" + date_type,
            ]
            df_ind = df_ind.merge(
                df_add_to_basket_count_int_1, how="left", on="GUID_PJ"
            )

        # Days Since Last Search for the journey and overall
        df_past_1 = df_past_1.sort_index()
        today = df_past_1[df_past_1["date_key"] == i]
        for level in levels:
            df_past_dsls = df_past_1.copy()
            df_past_dsls = df_past_dsls[df_past_dsls["browse_count_" + str(level)] > 0]
            df_past_dsls = df_past_dsls.append(today)
            df_past_dsls = df_past_dsls.drop_duplicates()
            df_past_dsls["prev_value_journey"] = df_past_dsls.groupby("GUID_PJ")[
                "date_key"
            ].shift(1)
            df_past_dsls["Days_since_last_search_" + level] = (
                df_past_dsls["date_key"] - df_past_dsls["prev_value_journey"]
            )
            df_past_dsls = df_past_dsls[df_past_dsls["date_key"] == i][
                ["GUID_PJ", "Days_since_last_search_" + str(level)]
            ]
            df_ind = df_ind.merge(df_past_dsls, how="left", on="GUID_PJ")

        # Days Since Last ADD TO CART
        for level in levels:
            df_past_2 = df_past_1.copy()
            df_past_2 = df_past_2[df_past_2["add_to_basket_" + level] > 0]
            df_past_2 = df_past_2.append(today)
            df_past_2 = df_past_2.drop_duplicates()
            df_past_2["prev_value_addtobasket"] = df_past_2.groupby("GUID_PJ")[
                "date_key"
            ].shift(1)
            df_past_2["Days_since_last_addtobasket_" + level] = (
                df_past_2["date_key"] - df_past_2["prev_value_addtobasket"]
            )
            df_past_2 = df_past_2[["GUID_PJ", "Days_since_last_addtobasket_" + level]][
                df_past_2["date_key"] == i
            ]
            df_ind = df_ind.merge(df_past_2, how="left", on="GUID_PJ")

        # Distinct Days Searched/Days Since First Search
        for level in levels:
            df_search = df_past_1.copy()
            df_search = df_search[(df_search["browse_count_" + level] > 0)]
            df_search = df_search.append(today)
            df_search = df_search.drop_duplicates()

            dumy = pd.to_datetime(i)
            # df_search['date_key']=df_search['date_key'].apply(lambda x: pd.to_datetime(str(x)))
            # df_search['date_key']=df_search['date_key'].dt.date

            df_search = df_search.groupby(["GUID_PJ"])["date_key"].agg(
                lambda x: (x.nunique() - 1) / ((dumy - x.min()).days + 1)
            )
            df_search = df_search.reset_index()
            df_search.columns = [
                "GUID_PJ",
                "Distinct_Days_by_Days_first_search_" + level,
            ]
            df_ind = df_ind.merge(df_search, how="left", on="GUID_PJ")

        # Number of searches/average searches of all guid_keys
        df_past_nsas = (
            df_past_1[["browse_count_journey", "browse_count_OJ", "GUID_PJ"]]
            .groupby(["GUID_PJ"])
            .sum()
            / df_past_1[["browse_count_journey", "browse_count_OJ", "GUID_PJ"]]
            .groupby(["GUID_PJ"])
            .sum()
            .mean()
        )
        df_past_nsas = df_past_nsas.reset_index()
        df_past_nsas.columns = [
            "GUID_PJ",
            "searches_on_category_by_avg_searches_journey",
            "searches_on_category_by_avg_searches_OJ",
        ]
        df_ind = df_ind.merge(df_past_nsas, how="left", on="GUID_PJ")

        # Average Browse Time
        dwell_time = ["dwell_time_seconds_journey", "dwell_time_seconds_OJ"]
        df_average_time = df_past_1.copy()
        df_average_time = df_average_time[
            [
                "GUID_PJ",
                "date_key",
                "dwell_time_seconds_journey",
                "dwell_time_seconds_OJ",
            ]
        ]
        for date_type in date_list:
            df_average_time_int = df_average_time[
                df_average_time["date_key"] >= dates_dict[date_type]
            ]
            df_average_time_1 = df_average_time_int.copy()
            df_average_time_1[dwell_time] = df_average_time_1[dwell_time].replace(
                [0, 0.0], np.nan
            )
            df_average_time_2 = (
                df_average_time_1.groupby("GUID_PJ")[dwell_time].mean().reset_index()
            )
            df_average_time_2.columns = ["GUID_PJ"] + [
                j + "_" + date_type for j in dwell_time
            ]
            df_ind = df_ind.merge(df_average_time_2, how="left", on="GUID_PJ")

        # Dwelling time on the category/Average dwelling time on the category
        df_average_time_1 = (
            df_past_1[
                ["GUID_PJ", "dwell_time_seconds_journey", "dwell_time_seconds_OJ"]
            ]
            .groupby(["GUID_PJ"])
            .sum()
            / df_past_1[
                ["GUID_PJ", "dwell_time_seconds_journey", "dwell_time_seconds_OJ"]
            ]
            .groupby(["GUID_PJ"])
            .sum()
            .mean()
        )
        df_average_time_1 = df_average_time_1.reset_index()
        df_average_time_1.columns = [
            "GUID_PJ",
            "dtime_on_category_by_avg_dtime_journey",
            "dtime_on_category_by_avg_dtime_OJ",
        ]
        df_ind = df_ind.merge(df_average_time_1, how="left", on="GUID_PJ")

        # Number of distinct_products browsed for last 7,14
        df_past_prod = df_past_1.copy()
        df_past_prod[
            ["product_array_all_journey", "product_array_all_OJ"]
        ] = df_past_prod[["product_array_all_journey", "product_array_all_OJ"]].astype(
            str
        )
        category_list = ["journey", "OJ"]
        for date_type in date_list:
            df_past_prod_int = df_past_prod[
                df_past_prod["date_key"] >= dates_dict[date_type]
            ]
            for category in levels:
                df_past_prod_1 = df_past_prod_int.copy()

                df_past_prod_1["array_" + date_type + "_" + category] = (
                    (df_past_prod_1["product_array_all_" + category] + ",")
                    .groupby(df_past_prod_1["GUID_PJ"])
                    .apply(lambda x: x.cumsum())
                    .str[:-1]
                )
                df_past_prod_1[
                    "array_split_" + date_type + "_" + category
                ] = df_past_prod_1["array_" + str(date_type) + "_" + category].apply(
                    lambda x: x.split(",")
                )
                df_past_prod_1["distinct_count_" + date_type + "_" + category] = [
                    len(set(x)) - 1 if "nan" in x else len(set(x))
                    for x in df_past_prod_1["array_split_" + date_type + "_" + category]
                ]
                df_past_prod_1["total_count_" + date_type + "_" + category] = [
                    len([j for j in x if j != "nan"])
                    for x in df_past_prod_1["array_split_" + date_type + "_" + category]
                ]
                df_past_prod_1 = df_past_prod_1[df_past_prod_1["date_key"] == i]
                df_past_prod_1 = df_past_prod_1[
                    [
                        "GUID_PJ",
                        "distinct_count_" + date_type + "_" + category,
                        "total_count_" + date_type + "_" + category,
                    ]
                ]
                df_ind = df_ind.merge(df_past_prod_1, how="left", on="GUID_PJ")

        df_ind_2 = df_2.groupby(["GUID_PJ"]).sum()
        df_independent = df_ind.merge(df_ind_2, how="left", on="GUID_PJ")

        #####################################################################################################
        # final_concating
        modelling_df = df_independent
        modelling_df["dataframe_date_key"] = date
        return modelling_df
