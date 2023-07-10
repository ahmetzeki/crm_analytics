import datetime
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan
# müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
# 1. flo_data_20K.csv verisini okuyunuz.
import datetime as dt
import pandas as pd
import time
import warnings

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 600)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()
# 2. Veri setinde
# a. İlk 10 gözlem,
# b. Değişken isimleri,
# c. Betimsel istatistik,
# d. Boş değer,
# e. Değişken tipleri, incelemesi yapınız.
df.head(10)
df.columns
df.dtypes
df.shape
df.isnull().sum()
df.describe().T
# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Herbir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.

df["total_order_num"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["total_customer_value"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
## Metod1
for col in df.columns:  # 0.07s
    if "date" in col:
        df[col] = pd.to_datetime(df[col])

## Metod2
[pd.to_datetime(df[col]) for col in df.columns if "date" in col]  # 0.03s

## Metod3
[df[col].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d')) for col in df.columns if "date" in col]  # 1.45s

# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının
# ve ortalama harcamaların dağılımına bakınız.

df.groupby("order_channel").agg({"master_id": ["count"],
                                 "total_order_num": "mean",
                                 "total_customer_value": "mean"})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df[["master_id", "total_customer_value"]].sort_values("total_customer_value", ascending=False).head(10)

# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df[["master_id", "total_order_num"]].sort_values("total_order_num", ascending=False).head(10)


# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

def change_date_types(dataframe):
    '''Changes a variable types of dates from object to datetime '''

    dataframe["total_order_num"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["total_customer_value"] = dataframe["customer_value_total_ever_offline"] + dataframe[
        "customer_value_total_ever_online"]
    for col in dataframe.columns:  # 0.07s
        if "date" in col:
            dataframe[col] = pd.to_datetime(dataframe[col])

    return dataframe


def analyze_df(dataframe, avg_order_and_value=False, show_best_values=False, show_most_orders=False):
    if avg_order_and_value:
        return dataframe.groupby("order_channel").agg({"master_id": ["count"],"total_order_num": "mean",
                                                       "total_customer_value": "mean"})
    if show_best_values:
        return dataframe[["master_id", "total_customer_value"]].sort_values("total_customer_value",
                                                                            ascending=False).head(10)
    if show_most_orders:
        return dataframe[["master_id", "total_order_num"]].sort_values("total_order_num", ascending=False).head(10)


# calling functions
df_with_updated_date = change_date_types(df)
analyze_df(df_with_updated_date, avg_order_and_value=True)
# GÖREV 2: RFM Metriklerinin Hesaplanması

# Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi

df["last_order_date"].max()

today_date = dt.datetime(2021, 6, 1)  # y-m-d

# customer_id, recency, frequency ve monetary değerlerinin yer aldığı yeni bir rfm dataframe

rfm = df.groupby("master_id").agg({"last_order_date": lambda x: (today_date - x.max()).days,
                                   "total_order_num": lambda x: x,
                                   "total_customer_value": lambda x: x})

rfm.head()
rfm.columns = ["recency", "frequency", "monetary"]
# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

# Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çevrilmesi ve
# Bu skorları recency_score, frequency_score ve monetary_score olarak kaydedilmesi

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

# recency_score ve frequency_score’u tek bir değişken olarak ifade edilmesi ve RF_SCORE olarak kaydedilmesi

rfm["RFM_SCORE"] = (rfm["recency_score"].astype(str) +
                    rfm["frequency_score"].astype(str))
# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

# Oluşturulan RFM skorların daha açıklanabilir olması için segment tanımlama
# ve  tanımlanan seg_map yardımı ile RF_SCORE'u segmentlere çevirme

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

# GÖREV 5: Aksiyon zamanı!
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm[['segment', 'recency', 'frequency', 'monetary']].groupby("segment").agg(["mean"])

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
# yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.


df['avg_price_for_per_item'] = df['total_customer_value'] / df['total_order_num']  # ortalama değer sütunu

rfm = pd.merge(rfm, df[['interested_in_categories_12', 'master_id', 'avg_price_for_per_item']], on='master_id')

rfm_women = rfm[((rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")) &
                (rfm['avg_price_for_per_item'] > 250) &
                (rfm['interested_in_categories_12'].str.contains('KADIN'))].reset_index()

rfm_women.drop('index', axis=1)
rfm_women['master_id'].to_csv("yeni_marka_hedef_müşteri_id", index=False)
# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.
rfm_men_and_children = rfm[
    ((rfm["segment"] == "cant_loose") | (rfm["segment"] == "about_to_sleep") | (rfm["segment"] == "new_customers")) &
    ((rfm['interested_in_categories_12'].str.contains('ERKEK')) | (
        rfm['interested_in_categories_12'].str.contains('COCUK')))].reset_index()


# GÖREV 6: Tüm süreci fonksiyonlaştırınız.
