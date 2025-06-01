import requests
import json
import pandas as pd
import time

class quiver:
    def __init__(self, token):
        self.token = token
        self.headers = {'accept': 'application/json',
        'X-CSRFToken': 'TyTJwjuEC7VV7mOqZ622haRaaUr0x0Ng4nrwSRFKQs7vdoBcJlK9qjAS69ghzhFu',
        'Authorization': "Token "+self.token}
    
    def congress_trading(self, ticker="", politician=False, recent=True, page="", page_size=""):
        if recent:
            urlStart = 'https://api.quiverquant.com/beta/live/congresstrading'
        else:
            urlStart = 'https://api.quiverquant.com/beta/bulk/congresstrading'
        if politician:
            ticker = ticker.replace(" ", "%20")
            url = urlStart+"?representative="+ticker
            
        elif len(ticker)>0:
            urlStart = 'https://api.quiverquant.com/beta/historical/congresstrading'
            url = urlStart+"/"+ticker
        else:
            url = urlStart
        
        r = requests.get(url, headers=self.headers, params={"page": page, "page_size": page_size})

        if ("QueuePool" in r.text or "Gateway" in r.text or "seconds" in r.text) and r.status_code != 200:
            num_seconds = 1
            if "seconds" in r.text:
                num_seconds = int(r.text.split(" seconds")[0].split(" ")[-1])
            print(f"Server overloaded. Sleeping for {num_seconds} seconds")
            
            time.sleep(num_seconds)
            return self.congress_trading(ticker, politician, recent, page, page_size)
        
        try:
            df = pd.DataFrame(json.loads(r.content))
        except:
            print(f"PAGE {page} ERROR")
            print(r.text)
            return pd.DataFrame()

        if (len(df)==0) or (df.shape[0]==0):
            print("No results found")
            return df
        
        # Handle different API response structures
        # New API uses "Traded" and "Filed" fields, old one used "TransactionDate" and "ReportDate"
        if "Traded" in df.columns:
            df["Traded"] = pd.to_datetime(df["Traded"])
        if "Filed" in df.columns:
            df["Filed"] = pd.to_datetime(df["Filed"])
        elif "ReportDate" in df.columns:
            df["ReportDate"] = pd.to_datetime(df["ReportDate"])
        if "TransactionDate" in df.columns:
            df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])

        # Process trade size if present
        if "Trade_Size_USD" in df.columns and df["Trade_Size_USD"].notna().any():
            df = df[df["Trade_Size_USD"].notna()]
            df["Trade_Size_USD"] = df["Trade_Size_USD"].astype(str).apply(lambda x: x.split(' - ')[-1])
            df["Trade_Size_USD"] = df["Trade_Size_USD"].replace('[^0-9.]', '', regex=True).astype(float)
        elif "Amount" in df.columns and df["Amount"].notna().any():
            df = df[df["Amount"].notna()]
            # Handle Amount field if Trade_Size_USD is not present
            if pd.api.types.is_numeric_dtype(df["Amount"]):
                pass  # Already numeric
            else:
                try:
                    df["Amount"] = df["Amount"].astype(float)
                except:
                    # Handle string ranges like in Trade_Size_USD
                    df["Amount"] = df["Amount"].astype(str).apply(lambda x: x.split(' - ')[-1] if ' - ' in x else x)
                    df["Amount"] = df["Amount"].replace('[^0-9.]', '', regex=True).astype(float)

        # Apply pagination manually if page_size is provided
        # This is needed because the API might not respect the page_size parameter
        if page_size:
            try:
                page_size = int(page_size)
                if page:
                    page = int(page)
                    start_idx = (page - 1) * page_size
                    df = df.iloc[start_idx:start_idx + page_size].copy()
                else:
                    df = df.iloc[:page_size].copy()
            except (ValueError, TypeError) as e:
                print(f"Warning: Error applying pagination: {e}")

        return df
   

    def senate_trading(self, ticker=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/senatetrading/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/senatetrading"
        
        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            if (len(df)==0) or (df.shape[0]==0):
                print("No results found")
                return df
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            print(f"Error processing senate trading data: {e}")
            return pd.DataFrame()

    def house_trading(self, ticker=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/housetrading/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/housetrading"
        
        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            if (len(df)==0) or (df.shape[0]==0):
                print("No results found")
                return df
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            print(f"Error processing house trading data: {e}")
            return pd.DataFrame()    
    
    def offexchange(self, ticker=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/offexchange/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/offexchange"
        
        try:
            r = requests.get(url, headers=self.headers)
            j = json.loads(r.content)
            df = pd.DataFrame(j)
            
            if (len(df)==0) or (df.shape[0]==0):
                print("No results found")
                return df
                
            if len(ticker)>0 and "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
                
            return df
        except Exception as e:
            print(f"Error processing offexchange data: {e}")
            return pd.DataFrame()
    
    def gov_contracts(self, ticker="", page="", page_size=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/govcontractsall/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/govcontractsall"

        r = requests.get(url, headers=self.headers, params={"page": page, "page_size": page_size})
        
        if ("QueuePool" in r.text or "Gateway" in r.text) and r.status_code != 200:
            print("server overloaded")
            time.sleep(1)
            return self.gov_contracts(ticker, page, page_size)
        
        try:
            df = pd.DataFrame(json.loads(r.content))
        except:
            print(f"PAGE {page} ERROR")
            return pd.DataFrame()

        if (len(df)==0) or (df.shape[0]==0):
            print("No results found")
            return df
            
        # Apply pagination manually if page_size is provided
        # This is needed because the API might not respect the page_size parameter
        if page_size:
            try:
                page_size = int(page_size)
                if page:
                    page = int(page)
                    start_idx = (page - 1) * page_size
                    df = df.iloc[start_idx:start_idx + page_size].copy()
                else:
                    df = df.iloc[:page_size].copy()
            except (ValueError, TypeError) as e:
                print(f"Warning: Error applying pagination: {e}")
                
        return df

    
    def lobbying(self, ticker=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/lobbying/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/lobbying"

        r = requests.get(url, headers=self.headers)
        
        try:
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
                print("No results found")
                return df
                
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            
            return df
        except Exception as e:
            print(f"Error processing lobbying data: {e}")
            return pd.DataFrame()
        
    def insiders(self, ticker=""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/live/insiders?ticker="+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/insiders"
         
        try:
            r = requests.get(url, headers=self.headers)
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/wikipedia/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/wikipedia"

        try:
            r = requests.get(url, headers=self.headers)
            
            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')
                
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
    
    def wallstreetbets(self, ticker="",date_from = "", date_to = "", yesterday=False):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/wallstreetbets/"+ticker

        else:
            url = "https://api.quiverquant.com/beta/live/wallstreetbets?count_all=true"

            if len(date_from)>0:
                date_from = pd.to_datetime(date_from).strftime('%Y%m%d')
                url = url+"&date_from="+date_from 
            if len(date_to)>0:
                date_to = pd.to_datetime(date_to).strftime('%Y%m%d')
                url = url+"&date_to="+date_to 

        if yesterday:
            url = "https://api.quiverquant.com/beta/live/wallstreetbets"

        try:
            r = requests.get(url, headers=self.headers)

            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')

            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
                print("No results found")
                return df

            if not yesterday:
                try:
                    if "Time" in df.columns:
                        df["Date"] = pd.to_datetime(df["Time"], unit='ms')
                    elif "Date" in df.columns:
                        df["Date"] = pd.to_datetime(df["Date"])
                        
                    if len(date_from)>0:
                        df = df[df["Date"]>=pd.to_datetime(date_from)]
                    if len(date_to)>0:
                        df = df[df["Date"]<=pd.to_datetime(date_to)]
                except Exception as e:
                    print(f"Error processing date data: {e}")

            return df
        except Exception as e:
            if "Upgrade your subscription" in str(e):
                raise
            print(f"Error processing WallStreetBets data: {e}")
            return pd.DataFrame() 
    
    def twitter(self, ticker = ""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/twitter/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/twitter"

        try:
            r = requests.get(url, headers=self.headers)
            
            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')
                
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
    
    def spacs(self, ticker = ""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/spacs/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/spacs"

        try:
            r = requests.get(url, headers=self.headers)
            
            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')
                
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
    
    def flights(self, ticker = ""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/flights/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/flights"

        try:
            r = requests.get(url, headers=self.headers)
            
            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')
                
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
        
        
    def political_beta(self, ticker = ""):
        if len(ticker)>0:
            url = "https://api.quiverquant.com/beta/historical/politicalbeta/"+ticker
        else:
            url = "https://api.quiverquant.com/beta/live/politicalbeta"

        try:
            r = requests.get(url, headers=self.headers)
            
            if r.text == '"Upgrade your subscription plan to access this dataset."':
                raise NameError('Upgrade your subscription plan to access this dataset.')
                
            df = pd.DataFrame(json.loads(r.content))
            
            if (len(df)==0) or (df.shape[0]==0):
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
    
    def patents(self, ticker = ""):
        if len(ticker)<1:
            url = "https://api.quiverquant.com/beta/live/allpatents"
        else:
            url = "https://api.quiverquant.com/beta/historical/allpatents/"+ticker
       
        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset.')
        
        df = pd.DataFrame(json.loads(r.content))
        df["Date"] = pd.to_datetime(df["Date"])
        
        return df
    
    ## Contact chris@quiverquant.com about access to these functions
    def sec13F(self, ticker="", date="", owner="", period=""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/sec13f"
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(date)>0:
            url = url+separator+"date="+date
            separator = "&"
        if len(owner)>0:
            url = url+separator+"owner="+owner
            separator="&"
        if len(period)>0:
            url = url+separator+"period="+period
            separator="&"
            
        print("Pulling data from: ", url)     
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset.')
            
        df = pd.DataFrame(json.loads(r.content))
        df["ReportPeriod"] = pd.to_datetime(df["ReportPeriod"], unit='ms')
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
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(date)>0:
            url = url+separator+"date="+date
            separator = "&"
        if len(owner)>0:
            url = url+separator+"owner="+owner
            separator="&"
        if len(period)>0:
            url = url+separator+"period="+period
            separator="&"
            
        print("Pulling data from: ", url)     
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset.')
            
        df = pd.DataFrame(json.loads(r.content))
        df["ReportPeriod"] = pd.to_datetime(df["ReportPeriod"], unit='ms')
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
    
    def wallstreetbetsComments(self, ticker="", freq="", date_from = "", date_to = ""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/wsbcomments"
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(freq)>0:
            url = url+separator+"freq="+freq
            separator = "&"
        if len(date_from)>0:
            url = url+separator+"date_from="+date_from
            separator = "&"   
        if len(date_to)>0:
            url = url+separator+"date_to="+date_to
            separator = "&"   
            
        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions.')
            
        df = pd.DataFrame(json.loads(r.content))
        df['Datetime'] = pd.to_datetime(df["Time"], unit='ms')
        return df 
    
    def wallstreetbetsCommentsFull(self, ticker="", freq="", date_from = "", date_to = ""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/wsbcommentsfull"
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(freq)>0:
            url = url+separator+"freq="+freq
            separator = "&"
        if len(date_from)>0:
            url = url+separator+"date_from="+date_from
            separator = "&"   
        if len(date_to)>0:
            url = url+separator+"date_to="+date_to
            separator = "&"   
            
        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions.')
            
        df = pd.DataFrame(json.loads(r.content))
        df['Datetime'] = pd.to_datetime(df["Time"], unit='ms')
        return df 
    
       
    def cryptoComments(self, ticker="", freq="", date_from = "", date_to = ""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/cryptocomments"
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(freq)>0:
            url = url+separator+"freq="+freq
            separator = "&"
        if len(date_from)>0:
            url = url+separator+"date_from="+date_from
            separator = "&"   
        if len(date_to)>0:
            url = url+separator+"date_to="+date_to
            separator = "&"   
            
        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)
        
        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions.')
            
        df = pd.DataFrame(json.loads(r.content))
        df['Datetime'] = pd.to_datetime(df["Time"], unit='ms')
        return df 
    
    def cryptoCommentsHistorical(self, ticker="", freq="", date_from = "", date_to = ""):
        separator = "?"
        url = "https://api.quiverquant.com/beta/live/cryptocommentsfull"
        if len(ticker)>0:
            url = url+separator+"ticker="+ticker
            separator = "&"
        if len(freq)>0:
            url = url+separator+"freq="+freq
            separator = "&"
        if len(date_from)>0:
            url = url+separator+"date_from="+date_from
            separator = "&"   
        if len(date_to)>0:
            url = url+separator+"date_to="+date_to
            separator = "&"   

        print("Pulling data from: ", url)
        r = requests.get(url, headers=self.headers)

        if r.text == '"Upgrade your subscription plan to access this dataset."':
            raise NameError('Upgrade your subscription plan to access this dataset. Contact chris@quiverquant.com with questions.')

        df = pd.DataFrame(json.loads(r.content))
        df['Datetime'] = pd.to_datetime(df["Time"], unit='ms')
        return df 