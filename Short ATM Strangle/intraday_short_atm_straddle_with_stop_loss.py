import pandas as pd
import numpy as np


# Fetch CSV file and convert it into a dataframe

atm_put_path = '/Users/vamsikrishnasb/Trading/Analysis/GitHub/short_atm_straddle/short_atm_puts.csv'
atm_call_path = '/Users/vamsikrishnasb/Trading/Analysis/GitHub/short_atm_straddle/short_atm_calls.csv'

atm_put_df = pd.read_csv(atm_put_path)
atm_call_df = pd.read_csv(atm_call_path)

atm_put_df = atm_put_df.drop_duplicates()
atm_call_df = atm_call_df.drop_duplicates()


# Puts

puts = atm_put_df

temp = puts.groupby(['date'], sort=False)['days_to_expiry'].min()
temp = pd.DataFrame(temp)

puts = puts[['date', 'expiry', 'strike', 'days_to_expiry', 'open_option', 
             'high_option', 'low_option', 'close_option', 'open_ul', 'close_ul', 
             'implied_volatility', 'delta', 'gamma', 'theta', 'vega']]
puts = temp.merge(puts, on=['date', 'days_to_expiry'], how='inner')

temp = puts.groupby(['date'], sort=False)['strike'].max()
temp = pd.DataFrame(temp)

puts = temp.merge(puts, on=['date', 'strike'], how='inner')


# Calls

calls = atm_call_df

temp = calls.groupby(['date'], sort=False)['days_to_expiry'].min()
temp = pd.DataFrame(temp)
# temp

calls = calls[['date', 'expiry', 'strike', 'days_to_expiry', 'open_option', 
             'high_option', 'low_option', 'close_option', 'open_ul', 'close_ul', 
             'implied_volatility', 'delta', 'gamma', 'theta', 'vega']]
calls = temp.merge(calls, on=['date', 'days_to_expiry'], how='inner')

temp = calls.groupby(['date'], sort=False)['strike'].min()
temp = pd.DataFrame(temp)

calls = temp.merge(calls, on=['date', 'strike'], how='inner')


# Strangle

atm_strangle = puts.merge(calls, left_on='date', right_on='date')

atm_strangle['price'] = atm_strangle['open_option_x'] + atm_strangle['open_option_y']

atm_strangle['sl_put'] = (
    atm_strangle['high_option_x'] - atm_strangle['open_option_x']
) + (
    atm_strangle['low_option_y'] - atm_strangle['open_option_y']
)

atm_strangle['sl_call'] = (
    atm_strangle['low_option_x'] - atm_strangle['open_option_x']
) + (
    atm_strangle['high_option_y'] - atm_strangle['open_option_y']
)

atm_strangle['stop_loss'] = 0.5 * atm_strangle['price']

c1 = ((atm_strangle['sl_put'] <= atm_strangle['stop_loss']) 
      | (atm_strangle['sl_call'] <= atm_strangle['stop_loss']))
c2 = ((atm_strangle['sl_put'] > atm_strangle['stop_loss']) 
      & (atm_strangle['sl_call'] > atm_strangle['stop_loss']))


pnl = [((atm_strangle['open_option_x'] - atm_strangle['close_option_x'])
       + (atm_strangle['open_option_y'] - atm_strangle['close_option_y'])), 
       - atm_strangle['stop_loss']]
default=0

atm_strangle['final_pnl'] = np.select([c1, c2], pnl, default=default)



atm_strangle['date'] = pd.to_datetime(atm_strangle['date'])
atm_strangle['expiry_x'] = pd.to_datetime(atm_strangle['expiry_x'])
atm_strangle['week'] = atm_strangle['date'].dt.week
atm_strangle['month'] = atm_strangle['date'].dt.month
atm_strangle['month'] = atm_strangle['month'].apply(lambda x: '{0:0>1}'.format(x))
atm_strangle['year'] = atm_strangle['date'].dt.year
atm_strangle['year'] = atm_strangle['year'].astype(str)
atm_strangle['mmyyyy'] = atm_strangle['month'].astype(str) + atm_strangle['year'].astype(str)



# Final PnL Calculation | Total

print("Net PnL since Feb 2019:", atm_strangle['final_pnl'].sum())


# Final PnL Calculation | Yearly

print("Yearly PnL since Feb 2019", atm_strangle.groupby('year').final_pnl.agg(['sum']))


# Final PnL Calculation | Monthly

mmyy_pnl = pd.DataFrame(atm_strangle.groupby(['year', 'month']).final_pnl.agg(['sum']))
print("Monthly PnL since Feb 2019", mmyy_pnl.to_string())