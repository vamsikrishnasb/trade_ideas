import pandas as pd
import numpy as np


# Fetch CSV files and convert them into dataframes

short_atm_path = '/Users/vamsikrishnasb/Trading/Analysis/GitHub/short_atm_put_long_otm_puts/short_atm_puts.csv'
long_otm_path = '/Users/vamsikrishnasb/Trading/Analysis/GitHub/short_atm_put_long_otm_puts/long_otm_puts.csv'
square_off_path = '/Users/vamsikrishnasb/Trading/Analysis/GitHub/short_atm_put_long_otm_puts/square_off.csv'

puts_atm_df = pd.read_csv(short_atm_path)
puts_otm_df = pd.read_csv(long_otm_path)
square_off_df = pd.read_csv(square_off_path)

puts_atm_df = puts_atm_df.drop_duplicates()
puts_otm_df = puts_otm_df.drop_duplicates()
square_off_df = square_off_df.drop_duplicates()


# Short ATM Puts | PnL Calculation

puts_atm = puts_atm_df[['date', 'expiry', 'strike', 'close_option', 'close_ul', 'days_to_expiry']]

square_off = square_off_df[['expiry', 'strike', 'close_ul']]

puts_atm = puts_atm.rename(columns={
    'date': 'date_start_atm', 'expiry': 'expiry_start_atm', 'strike': 'strike_start_atm', 
    'close_option': 'close_option_start_atm', 'close_ul': 'close_ul_start_atm', 
    'days_to_expiry': 'days_to_expiry_start_atm'})
square_off = square_off.rename(columns={
    'expiry': 'expiry_start_atm', 'strike': 'strike_start_atm', 
    'close_ul': 'close_ul_end'})


temp = puts_atm.groupby(['expiry_start_atm'], sort=False)['days_to_expiry_start_atm'].min()
temp = pd.DataFrame(temp)

puts_atm = temp.merge(puts_atm, on=['expiry_start_atm', 'days_to_expiry_start_atm'], how='inner')

temp = puts_atm.groupby(['expiry_start_atm'], sort=False)['strike_start_atm'].max()
temp = pd.DataFrame(temp)

puts_atm = temp.merge(puts_atm, on=['expiry_start_atm', 'strike_start_atm'], how='inner')
puts_atm = puts_atm.merge(square_off, on=['expiry_start_atm', 'strike_start_atm'], how='left')

puts_atm['k_minus_s'] = puts_atm['strike_start_atm'] - puts_atm['close_ul_end']

c1 = (puts_atm['k_minus_s'] <= 0)
c2 = (puts_atm['k_minus_s'] > 0)

pnl = [puts_atm['close_option_start_atm'], puts_atm['close_option_start_atm'] - puts_atm['k_minus_s']]
default=0

puts_atm['puts_pnl_atm'] = np.select([c1, c2], pnl, default=default)

puts_atm['expiry_start_atm'] = pd.to_datetime(puts_atm['expiry_start_atm'])

puts_atm['month'] = puts_atm['expiry_start_atm'].dt.month
puts_atm['year'] = puts_atm['expiry_start_atm'].dt.year

atm_pnl = puts_atm.groupby('year').puts_pnl_atm.agg(['sum'])


# Long OTM Puts | PnL Calculation

puts_otm = puts_otm_df[['date', 'expiry', 'strike', 'close_option', 'close_ul', 
                                             'days_to_expiry']]

puts_atm_to_merge = puts_atm[['expiry_start_atm', 'strike_start_atm', 'close_option_start_atm', 'puts_pnl_atm']]

square_off = square_off_df[['expiry', 'strike', 'close_ul']]

puts_otm = puts_otm.rename(columns={
    'date': 'date_start_otm', 'expiry': 'expiry_start_otm', 'strike': 'strike_start_otm', 
    'close_option': 'close_option_start_otm', 'close_ul': 'close_ul_start_otm', 
    'days_to_expiry': 'days_to_expiry_start_otm'})

puts_atm_to_merge = puts_atm_to_merge.rename(columns={
    'expiry_start_atm': 'expiry_start_otm'})

square_off = square_off.rename(columns={
    'expiry': 'expiry_start_otm', 'strike': 'strike_start_otm', 
    'close_ul': 'close_ul_end'})

square_off['expiry_start_otm'] = pd.to_datetime(square_off['expiry_start_otm'])

temp = puts_otm.groupby(['expiry_start_otm'], sort=False)['days_to_expiry_start_otm'].min()
temp = pd.DataFrame(temp)

puts_otm = temp.merge(puts_otm, on=['expiry_start_otm', 'days_to_expiry_start_otm'], how='inner')

temp = puts_otm.groupby(['expiry_start_otm'], sort=False)['strike_start_otm'].max()
temp = pd.DataFrame(temp)

puts_otm = temp.merge(puts_otm, on=['expiry_start_otm', 'strike_start_otm'], how='inner')

puts_otm['expiry_start_otm'] = pd.to_datetime(puts_otm['expiry_start_otm'])

puts_otm = puts_otm.merge(puts_atm_to_merge, on=['expiry_start_otm'], how='inner')

puts_otm['atm_otm_ratio'] = (puts_otm['close_option_start_atm'] 
                             / puts_otm['close_option_start_otm']).apply(np.floor)

puts_otm = puts_otm.merge(square_off, on=['expiry_start_otm', 'strike_start_otm'], how='left')

puts_otm['k_minus_s'] = puts_otm['strike_start_otm'] - puts_otm['close_ul_end']
puts_otm

c1 = (puts_otm['k_minus_s'] <= 0)
c2 = (puts_otm['k_minus_s'] > 0)

pnl = [- puts_otm['close_option_start_otm'] * puts_otm['atm_otm_ratio'] * 0.25,
       (puts_otm['k_minus_s'] - puts_otm['close_option_start_otm']) * puts_otm['atm_otm_ratio'] * 0.25]
default=0

puts_otm['puts_pnl_otm'] = np.select([c1, c2], pnl, default=default)



# Final PnL Calculation | Total

puts_otm['final_pnl'] = puts_otm['puts_pnl_atm'] + puts_otm['puts_pnl_otm']

puts_otm['month'] = puts_otm['expiry_start_otm'].dt.month
puts_otm['year'] = puts_otm['expiry_start_otm'].dt.year

final_pnl = puts_otm.groupby('year').final_pnl.agg(['sum'])

print("Total PnL since Dec 2010:", puts_otm['puts_pnl_otm'].sum())


# Final PnL Calculation | Yearly

print("Yearly PnL since Dec 2010", final_pnl)


# Final PnL Calculation | Monthly

mmyy_pnl_otm = pd.DataFrame(puts_otm.groupby(['year', 'month']).final_pnl.agg(['sum']))
print("Monthly PnL since Dec 2010", mmyy_pnl_otm.to_string())