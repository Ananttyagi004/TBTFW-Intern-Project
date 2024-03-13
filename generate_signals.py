import psycopg2
import pandas as pd

# Function to connect to PostgreSQL database
def connect_to_database():
    try:
        conn = psycopg2.connect(
             dbname="Stocks",
           user="postgres",
           password="anant",
           host="localhost",
            port="5432"

        )
        print("Connected to database successfully!")
        return conn
    except psycopg2.Error as e:
        print("Error connecting to database:", e)

# Function to retrieve stock data for a specific symbol
def retrieve_stock_data(conn, symbol):
    try:
        query = f"SELECT * FROM {symbol} ORDER BY Date"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print("Error retrieving stock data:", e)

# Function to generate buy signal
def generate_buy_signal(data):
    # Check if required columns are present
    required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
    missing_columns = set(required_columns) - set(data.columns)
    if missing_columns:
        print("Missing columns in data:", missing_columns)
        return None

    try:
        # Calculate moving averages
        data['50_MA'] = data['adj_close'].rolling(window=50).mean()
        data['500_MA'] = data['adj_close'].rolling(window=500).mean()
        
        # Generate buy signal
        data['Buy_Signal'] = (data['50_MA'] > data['500_MA']) & (data['50_MA'].shift(1) < data['500_MA'].shift(1))
        
        return data['Buy_Signal']
    except Exception as e:
        print("Error generating buy signal:", e)

# Function to generate sell signal
def generate_sell_signal(data):
    # Calculate 20-day and 200-day moving averages
    data['20_MA'] = data['adj_close'].rolling(window=20).mean()
    data['200_MA'] = data['adj_close'].rolling(window=200).mean()
    
    # Generate sell signal where 20-day MA crosses below 200-day MA
    data['Sell_Signal'] = (data['20_MA'] < data['200_MA']) & (data['20_MA'].shift(1) > data['200_MA'].shift(1))
    
    return data['Sell_Signal']

# Function to close buy positions
def close_buy_positions(data):
    # Calculate 10-day and 20-day moving averages
    data['10_MA'] = data['adj_close'].rolling(window=10).mean()
    data['20_MA'] = data['adj_close'].rolling(window=20).mean()
    
    # Generate close buy signal where 10-day MA crosses below 20-day MA
    data['Close_Buy'] = (data['10_MA'] < data['20_MA']) & (data['10_MA'].shift(1) > data['20_MA'].shift(1))
    
    return data['Close_Buy']

# Function to close sell positions
def close_sell_positions(data):
    # Calculate 5-day and 10-day moving averages
    data['5_MA'] = data['adj_close'].rolling(window=5).mean()
    data['10_MA'] = data['adj_close'].rolling(window=10).mean()
    
    # Generate close sell signal where 5-day MA crosses below 10-day MA
    data['Close_Sell'] = (data['5_MA'] < data['10_MA']) & (data['5_MA'].shift(1) > data['10_MA'].shift(1))
    
    return data['Close_Sell']

# def calculate_profit_loss(data, buy_signal, sell_signal):
#     # Initialize variables
#     buy_price = None
#     sell_price = None
#     position = None
#     profit_loss = 0

#     # Iterate through the DataFrame
#     for index, row in data.iterrows():
#         if buy_signal[index]:
#             if position != 'BUY':
#                 buy_price = row['Adj_Close']
#                 position = 'BUY'
#                 print("Bought at:", buy_price)
#         elif sell_signal[index]:
#             if position == 'BUY':
#                 sell_price = row['Adj_Close']
#                 position = None
#                 print("Sold at:", sell_price)
#                 profit_loss += (sell_price - buy_price)
#                 buy_price = None
#                 sell_price = None

#     # Close any remaining positions
#     if position == 'BUY':
#         sell_price = data.iloc[-1]['Adj_Close']  # Sell at the last price
#         print("Sold at:", sell_price)
#         profit_loss += (sell_price - buy_price)

#     return profit_loss

# Main function
def main():
    # Connect to database
    conn = connect_to_database()
    
    # Retrieve stock data for AAPL
    symbol = 'AAPL'
    stock_data = retrieve_stock_data(conn, symbol)
    
    # Generate signals
    buy_signal = generate_buy_signal(stock_data)
    sell_signal = generate_sell_signal(stock_data)
    close_buy = close_buy_positions(stock_data)
    close_sell = close_sell_positions(stock_data)

    # profit_loss = calculate_profit_loss(stock_data, buy_signal, sell_signal)
    # print("Overall Profit/Loss:", profit_loss)
    
    # Print signals
    print("Buy Signal:")
    print(buy_signal)
    print("\nSell Signal:")
    print(sell_signal)
    print("\nClose Buy Positions:")
    print(close_buy)
    print("\nClose Sell Positions:")
    print(close_sell)

if __name__ == "__main__":
    main()
