
import datetime as dt
import pandas as pd
import time
import warnings

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()


def check_df(dataframe, shape=False, columns=False, info=False, na=False, desc=False):
    """
    this func checks dataframe's
    :param dataframe: Pandas dataframe
    :param shape: checks shape of df
    :param columns: shows columns names
    :param info: prints columns types
    :param na: calculates NaN values in df
    :param desc: shows df's describtion
    """
    outputs = []
    if shape:
        outputs.append(('Shape', dataframe.shape))
    if columns:
        outputs.append(('Columns', dataframe.columns))
    if info:
        outputs.append(('Types', dataframe.dtypes))
    if na:
        outputs.append(('NA', dataframe.isnull().sum()))
    if desc:
        outputs.append(('Descriptive', dataframe.describe().T))
    for output in outputs:
        print(15 * "#", output[0], 15 * "#")
        print(output[1], "\n")


def change_date_types(dataframe):
    '''Summarizes online and offline orders, then changes a variable types of dates from object to datetime '''

    dataframe["total_order_num"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["total_customer_value"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]
    for col in dataframe.columns:
        if "date" in col:
            dataframe[col] = pd.to_datetime(dataframe[col])

    return dataframe


def analyze_df(dataframe, avg_order_and_value=False, show_best_values=False, show_most_orders=False):
    outputs = []
    if avg_order_and_value:
        print("avg orders and values are:")
        outputs.append(('"avg orders and values are:"', dataframe.groupby("order_channel").
                                                                    agg({"master_id": ["count"],
                                                                        "total_order_num": "mean",
                                                                        "total_customer_value": "mean"}).
                                                                                             reset_index().head()))
    if show_best_values:
        print("best customers are:")
        outputs.append(('best customers', dataframe.groupby("order_channel").
                                                                     agg({"master_id":  ["count"],
                                                                          "total_order_num": "mean",
                                                                          "total_customer_value": "mean"}).
                                                                                             reset_index().head()))
    if show_most_orders:
        print("most orders are:")
        outputs.append(('most orders are:', dataframe[["master_id", "total_order_num"]].
                                                   sort_values("total_order_num", ascending=False).reset_index().head()))

    return outputs


# calling functions


def rfm_metrix(dataframe, csv=False):
    dataframe["last_order_date"].max()
    today_date = dt.datetime(2021, 6, 1)  # y-m-d
    rfm = dataframe.groupby("master_id").agg({"last_order_date": lambda x: (today_date - x.max()).days,
                                              "total_order_num": lambda x: x,
                                              "total_customer_value": lambda x: x})

    rfm.columns = ["recency", "frequency", "monetary"]
    rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

    rfm["RFM_SCORE"] = (rfm["recency_score"].astype(str) +
                        rfm["frequency_score"].astype(str))

    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }

    rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)
    rfm[['segment', 'recency', 'frequency', 'monetary']].groupby("segment").agg(["mean"])

    df['avg_price_for_per_item'] = df['total_customer_value'] / df['total_order_num']  # ortalama değer sütunu

    rfm = pd.merge(rfm, df[['interested_in_categories_12', 'master_id', 'avg_price_for_per_item']], on='master_id')
    rfm_women = rfm[((rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")) &
                    (rfm['avg_price_for_per_item'] > 250) &
                    (rfm['interested_in_categories_12'].str.contains('KADIN'))].reset_index()

    rfm_women.drop('index', axis=1)
    rfm_women['master_id'].to_csv("yeni_marka_hedef_müşteri_id", index=False)

    rfm_men_and_children = rfm[((rfm["segment"] == "cant_loose") | (rfm["segment"] == "about_to_sleep") | (rfm["segment"] == "new_customers")) &
        ((rfm['interested_in_categories_12'].str.contains('ERKEK')) |
        (rfm['interested_in_categories_12'].str.contains('COCUK')))].reset_index()

    if csv:
        return rfm_women['master_id'].to_csv("yeni_marka_hedef_müşteri_id", index=False),
        rfm_men_and_children['master_id'].to_csv("target_customer", index=False)


check_df(df)
df_with_updated_dates = change_date_types(df)
analyze_df(df, show_best_values=True, show_most_orders=True)
rfm_metrix(df_with_updated_dates, csv=True)

