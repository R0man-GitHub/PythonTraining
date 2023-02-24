import pandas


class Analyze:
    def __init__(self, filename, out_filename):
        self.filename = filename
        self.file = pandas.read_csv(filename, sep=';')
        self.out_filename = out_filename

    def analyze_data(self):

        data_test = self.file
        data_hour = pandas.DataFrame()
        data_day = pandas.DataFrame()

        for date in self.file["trade_date"].unique():
            for hour in self.file["hour"].unique():
                data1 = data_test.loc[(data_test["hour"] == hour) & (data_test["trade_date"] == date)].copy()
                minmax = data1["oborot_v_denrax"].max() - data1["oborot_v_denrax"].min()
                data1["liquidity_rate"] = round(((data1["oborot_v_denrax"] - data1["oborot_v_denrax"].min()) / minmax) * 10)
                data1.loc[data1['liquidity_rate'] == 0, 'liquidity_rate'] = 1
                data_hour = pandas.concat([data_hour, data1])

            data2 = self.file.loc[self.file["trade_date"] == date].copy()
            data2 = data2.groupby(["trade_date", "asset_code"], as_index=False).sum().head(len(data2.index))
            minmax = data2["oborot_v_denrax"].max() - data2["oborot_v_denrax"].min()
            data2["liquidity_rate_day"] = round(((data2["oborot_v_denrax"] - data2["oborot_v_denrax"].min()) / minmax) * 10)
            data2.loc[data2['liquidity_rate_day'] == 0, 'liquidity_rate_day'] = 1
            data_day = pandas.concat([data_day, data2])

        data_test = data_test.merge(data_hour[["trade_date", "hour", "asset_code", "liquidity_rate"]], how='left', on=["trade_date", "hour", "asset_code"])
        data_test = data_test.merge(data_day[["trade_date", "asset_code", "liquidity_rate_day"]], how='left', on=["trade_date", "asset_code"])

        data_test.to_excel(self.out_filename)


res = Analyze("full_data.csv", "test2.xlsx")
res.analyze_data()
#full_data.csv
