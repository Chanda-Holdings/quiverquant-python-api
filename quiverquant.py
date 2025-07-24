import json
import time

import pandas as pd
import requests


class quiver:
    def __init__(self, token):
        self.token = token
        self.headers = {
            "accept": "application/json",
            "X-CSRFToken": "TyTJwjuEC7VV7mOqZ622haRaaUr0x0Ng4nrwSRFKQs7vdoBcJlK9qjAS69ghzhFu",
            "Authorization": "Token " + self.token,
        }

    def congress_trading(self, from_date):
        url = "https://api.quiverquant.com/beta/bulk/congresstrading"

        array_of_dataframes = []
        page = 1
        while True:
            r = requests.get(
                url, headers=self.headers, params={"page": page, "page_size": 1000}
            )
            if (
                "QueuePool" in r.text or "Gateway" in r.text or "seconds" in r.text
            ) and r.status_code != 200:
                num_seconds = 1
                if "seconds" in r.text:
                    num_seconds = int(r.text.split(" seconds")[0].split(" ")[-1])
                print(f"Server overloaded. Sleeping for {num_seconds} seconds")

                time.sleep(num_seconds)
                continue

            try:
                dataframe = pd.DataFrame(json.loads(r.content))
            except:
                print(f"PAGE {page} ERROR")
                print(r.text)
                raise

            if len(dataframe) == 0 or dataframe.shape[0] == 0:
                break

            array_of_dataframes.append(dataframe)
            page += 1

            dataframe["Traded"] = pd.to_datetime(dataframe["Traded"])
            if dataframe["Traded"].min() < pd.to_datetime(from_date).tz_localize(None):
                break

        df = pd.concat(array_of_dataframes, ignore_index=True)
        df = df[df["Traded"] >= pd.to_datetime(from_date).tz_localize(None)]

        if "Filed" in df.columns:
            df["Filed"] = pd.to_datetime(df["Filed"])

        numeric_cols = ["Trade_Size_USD", "Amount", "Value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def senate_trading(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/senatetrading/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/senatetrading"

        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            print(f"Error processing senate trading data: {e}")
            return pd.DataFrame()

    def senate_trading_old(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/senatetrading/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/senatetrading"

        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            print(f"Error processing senate trading data: {e}")
            return pd.DataFrame()

    def house_trading(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/housetrading/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/housetrading"

        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            print(f"Error processing house trading data: {e}")
            return pd.DataFrame()

    def offexchange(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/offexchange/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/offexchange"

        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            if len(ticker) > 0 and "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            print(f"Error processing offexchange data: {e}")
            return pd.DataFrame()

    def gov_contracts(self, from_date):
        url = "https://api.quiverquant.com/beta/live/govcontractsall"

        array_of_dataframes = []
        page = 1
        while True:
            r = requests.get(
                url, headers=self.headers, params={"page": page, "page_size": 1000}
            )

            if (
                "QueuePool" in r.text or "Gateway" in r.text or "seconds" in r.text
            ) and r.status_code != 200:
                num_seconds = 1
                if "seconds" in r.text:
                    num_seconds = int(r.text.split(" seconds")[0].split(" ")[-1])
                print(f"Server overloaded. Sleeping for {num_seconds} seconds")

                time.sleep(num_seconds)
                continue

            try:
                dataframe = pd.DataFrame(json.loads(r.content))
            except:
                print(f"PAGE {page} ERROR")
                print(r.text)
                raise

            if len(dataframe) == 0 or dataframe.shape[0] == 0:
                break

            array_of_dataframes.append(dataframe)
            page += 1

            dataframe["Date"] = pd.to_datetime(dataframe["Date"])
            if dataframe["Date"].min() < pd.to_datetime(from_date).tz_localize(None):
                break

        df = pd.concat(array_of_dataframes, ignore_index=True)
        df = df[df["Date"] >= pd.to_datetime(from_date).tz_localize(None)]

        return df

    def lobbying(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/lobbying/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/lobbying"

        r = requests.get(url, headers=self.headers)

        try:
            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            print(f"Error processing lobbying data: {e}")
            return pd.DataFrame()

    def insiders(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/live/insiders?ticker=" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/insiders"

        try:
            r = requests.get(url, headers=self.headers)
            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            if "fileDate" in df.columns:
                df["fileDate"] = pd.to_datetime(df["fileDate"])

            return df
        except Exception as e:
            print(f"Error processing insiders data: {e}")
            return pd.DataFrame()

    def wikipedia(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/wikipedia/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/wikipedia"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns if present
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing Wikipedia data: {e}")
            return pd.DataFrame()

    def wallstreetbets(self, ticker="", date_from="", date_to="", yesterday=False):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/wallstreetbets/" + ticker

        else:
            url = "https://api.quiverquant.com/beta/live/wallstreetbets?count_all=true"

            if len(date_from) > 0:
                date_from = pd.to_datetime(date_from).strftime("%Y%m%d")
                url = url + "&date_from=" + date_from
            if len(date_to) > 0:
                date_to = pd.to_datetime(date_to).strftime("%Y%m%d")
                url = url + "&date_to=" + date_to

        if yesterday:
            url = "https://api.quiverquant.com/beta/live/wallstreetbets"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            if not yesterday:
                try:
                    if "Time" in df.columns:
                        df["Date"] = pd.to_datetime(df["Time"], unit="ms")
                    elif "Date" in df.columns:
                        df["Date"] = pd.to_datetime(df["Date"])

                    if len(date_from) > 0:
                        df = df[df["Date"] >= pd.to_datetime(date_from)]
                    if len(date_to) > 0:
                        df = df[df["Date"] <= pd.to_datetime(date_to)]
                except Exception as e:
                    print(f"Error processing date data: {e}")

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing WallStreetBets data: {e}")
            return pd.DataFrame()

    def twitter(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/twitter/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/twitter"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns if present
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing Twitter data: {e}")
            return pd.DataFrame()

    def spacs(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/spacs/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/spacs"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns if present
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing SPACs data: {e}")
            return pd.DataFrame()

    def flights(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/flights/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/flights"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns if present
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            if "FlightDate" in df.columns:
                df["FlightDate"] = pd.to_datetime(df["FlightDate"])

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing flights data: {e}")
            return pd.DataFrame()

    def political_beta(self, ticker=""):
        if len(ticker) > 0:
            url = "https://api.quiverquant.com/beta/historical/politicalbeta/" + ticker
        else:
            url = "https://api.quiverquant.com/beta/live/politicalbeta"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError(
                    "Upgrade your subscription plan to access this dataset."
                )

            df = pd.DataFrame(json.loads(r.content))

            if (len(df) == 0) or (df.shape[0] == 0):
                print("No results found")
                return df

            # Handle date columns if present
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing political beta data: {e}")
            return pd.DataFrame()

        df = pd.DataFrame(json.loads(r.content))
        return df

    def patents(self, ticker=""):
        if len(ticker) < 1:
            url = "https://api.quiverquant.com/beta/live/allpatents"
        else:
            url = "https://api.quiverquant.com/beta/historical/allpatents/" + ticker

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError("Upgrade your subscription plan to access this dataset.")

        df = pd.DataFrame(json.loads(r.content))
        df["Date"] = pd.to_datetime(df["Date"])

        return df

    ## Contact chris@quiverquant.com about access to these functions
    def sec13F(self, ticker="", date="", owner="", period=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/sec13f"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(date) > 0:
            url = url + separator + "date=" + date
            separator = "&"
        if len(owner) > 0:
            url = url + separator + "owner=" + owner
            separator = "&"
        if len(period) > 0:
            url = url + separator + "period=" + period
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError("Upgrade your subscription plan to access this dataset.")

        df = pd.DataFrame(json.loads(r.content))
        df["ReportPeriod"] = pd.to_datetime(df["ReportPeriod"], unit="ms")
        #         #Can edit this out once we get past May 25th
        #         values = []
        #         for val in df["Value"]:
        #             if len(val)>=900:
        #                 values.append(val[:len(val)//1000])
        #             else:
        #                 values.append(val)
        #         df["Value"] = values
        ########

        #         df["Value"] = df["Value"].str.replace(",", "")
        #         df["Value"] = df["Value"].astype(float)

        #         df["Shares"] = df["Shares"].str.replace(",", "")
        #         df["Shares"] = df["Shares"].astype(float)
        df["Date"] = pd.to_datetime(df["Date"], unit="ms")
        return df

    def sec13FChanges(self, ticker="", date="", owner="", period=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/sec13fchanges"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(date) > 0:
            url = url + separator + "date=" + date
            separator = "&"
        if len(owner) > 0:
            url = url + separator + "owner=" + owner
            separator = "&"
        if len(period) > 0:
            url = url + separator + "period=" + period
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError("Upgrade your subscription plan to access this dataset.")

        df = pd.DataFrame(json.loads(r.content))
        df["ReportPeriod"] = pd.to_datetime(df["ReportPeriod"], unit="ms")
        #         #Can edit this out once we get past May 25th
        #         values = []
        #         for val in df["Value"]:
        #             if len(val)>=900:
        #                 values.append(val[:len(val)//1000])
        #             else:
        #                 values.append(val)
        #         df["Value"] = values
        ########

        #         df["Value"] = df["Value"].str.replace(",", "")
        #         df["Value"] = df["Value"].astype(float)

        #         df["Shares"] = df["Shares"].str.replace(",", "")
        #         df["Shares"] = df["Shares"].astype(float)
        df["Date"] = pd.to_datetime(df["Date"], unit="ms")
        return df

    def wallstreetbetsComments(self, ticker="", freq="", date_from="", date_to=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/wsbcomments"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(freq) > 0:
            url = url + separator + "freq=" + freq
            separator = "&"
        if len(date_from) > 0:
            url = url + separator + "date_from=" + date_from
            separator = "&"
        if len(date_to) > 0:
            url = url + separator + "date_to=" + date_to
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError(
                "Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions."
            )

        df = pd.DataFrame(json.loads(r.content))
        df["Datetime"] = pd.to_datetime(df["Time"], unit="ms")
        return df

    def wallstreetbetsCommentsFull(self, ticker="", freq="", date_from="", date_to=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/wsbcommentsfull"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(freq) > 0:
            url = url + separator + "freq=" + freq
            separator = "&"
        if len(date_from) > 0:
            url = url + separator + "date_from=" + date_from
            separator = "&"
        if len(date_to) > 0:
            url = url + separator + "date_to=" + date_to
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError(
                "Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions."
            )

        df = pd.DataFrame(json.loads(r.content))
        df["Datetime"] = pd.to_datetime(df["Time"], unit="ms")
        return df

    def cryptoComments(self, ticker="", freq="", date_from="", date_to=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/cryptocomments"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(freq) > 0:
            url = url + separator + "freq=" + freq
            separator = "&"
        if len(date_from) > 0:
            url = url + separator + "date_from=" + date_from
            separator = "&"
        if len(date_to) > 0:
            url = url + separator + "date_to=" + date_to
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError(
                "Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions."
            )

        df = pd.DataFrame(json.loads(r.content))
        df["Datetime"] = pd.to_datetime(df["Time"], unit="ms")
        return df

    def cryptoCommentsHistorical(self, ticker="", freq="", date_from="", date_to=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/cryptocommentsfull"
        if len(ticker) > 0:
            url = url + separator + "ticker=" + ticker
            separator = "&"
        if len(freq) > 0:
            url = url + separator + "freq=" + freq
            separator = "&"
        if len(date_from) > 0:
            url = url + separator + "date_from=" + date_from
            separator = "&"
        if len(date_to) > 0:
            url = url + separator + "date_to=" + date_to
            separator = "&"

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError(
                "Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions."
            )

        df = pd.DataFrame(json.loads(r.content))
        df["Datetime"] = pd.to_datetime(df["Time"], unit="ms")
        return df
