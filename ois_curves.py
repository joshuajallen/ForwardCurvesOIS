#
# Job to calculate 1d OIS forwards for estimating central bank pricing
#

from datetime import datetime
import logging
import os
import pandas as pd
import QuantLib as ql
from xbbg import blp

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)

logging.info("Started")

#
# Helper functions to calculate forwards
#
# C++ example for R maybe? http://mikejuniperhill.blogspot.com/2018/06/quantlib-bootstrapping-ois-curve.html
#

def fwd_curve(rates, terms, units, eval_date, currency):

    ql.Settings.instance().evaluationDate = ql.Date().from_date(eval_date)

    logging.info("Building OIS curve for %s on %s", currency, eval_date.strftime("%Y-%m-%d"))

    if currency == "EUR":
        settlement_days = 2
        calendar = ql.TARGET()
        day_count = ql.Actual360()
        index = ql.OvernightIndex("EUR", settlement_days, ql.EURCurrency(), calendar, day_count)
    elif currency == "GBP":
        settlement_days = 0
        calendar = ql.UnitedKingdom()
        day_count = ql.Actual365Fixed()
        index = ql.OvernightIndex("GBP", settlement_days, ql.GBPCurrency(), calendar, day_count)
    elif currency == "USD":
        settlement_days = 2
        calendar = ql.UnitedStates()
        day_count = ql.Actual360()
        index = ql.OvernightIndex("USD", settlement_days, ql.USDCurrency(), calendar, day_count)
    elif currency == "JPY":
        settlement_days = 2
        calendar = ql.Japan()
        day_count = ql.Actual365Fixed()
        index = ql.OvernightIndex("JPY", settlement_days, ql.JPYCurrency(), calendar, day_count)
    elif currency == "AUD":
        settlement_days = 1
        calendar = ql.Australia()
        day_count = ql.Actual365Fixed()
        index = ql.OvernightIndex("AUD", settlement_days, ql.AUDCurrency(), calendar, day_count)
    elif currency == "CAD":
        settlement_days = 1
        calendar = ql.Canada()
        day_count = ql.Actual365Fixed()
        index = ql.OvernightIndex("CAD", settlement_days, ql.CADCurrency(), calendar, day_count)
    else:
        logging.fatal("Unexpected OIS currency %s", currency)
        exit()

    ois_helpers = []

    for i in range(len(rates)):

        unit_char = units[i].upper()

        if unit_char == "D":
            time_unit = ql.Days
        elif unit_char == "W":
            time_unit = ql.Weeks
        elif unit_char == "M":
            time_unit = ql.Months
        elif unit_char == "Y":
            time_unit = ql.Years
        else:
            logging.fatal("Unexpected time unit found: %s", units[i])
            exit()

        tenor = ql.Period(int(terms[i]), time_unit)
        quote_handle = ql.QuoteHandle(ql.SimpleQuote(rates[i]/100))
        ois_helpers.append(ql.OISRateHelper(settlement_days, tenor, quote_handle, index))

    try:

        curve = ql.PiecewiseLogCubicDiscount(0, calendar, ois_helpers, day_count)

        all_days = ql.MakeSchedule(curve.referenceDate(), calendar.advance(curve.referenceDate(), 3, ql.Years), ql.Period("1d"))
        rates_fwd = [
        curve.forwardRate(d, calendar.advance(d, 1, ql.Days), day_count, ql.Continuous).rate()*100
        for d in all_days]
        forwards = pd.DataFrame()
        forwards["DateFwd"] = [datetime(d.year(), d.month(), d.dayOfMonth()) for d in all_days]
        forwards["RateFwd"] = rates_fwd
        ref_date = curve.referenceDate()
        forwards = forwards.assign(Date = datetime(ref_date.year(), ref_date.month(), ref_date.dayOfMonth()))
        forwards = forwards.assign(Currency = currency)
        forwards = forwards.assign(Updated = datetime.now())

        return(forwards[["Date", "Currency", "DateFwd", "RateFwd", "Updated"]])

    except RuntimeError as e:
        logging.error("Failed to build forward curve: %s", e)
        return(pd.DataFrame())


#
# Read some config data
#

os.chdir(os.path.dirname(os.path.abspath(__file__)))
logging.info("Working directory %s", os.getcwd())

config_file = "ois_curve_config.csv"
logging.info("Reading config file %s", config_file)
config = pd.read_csv(config_file)

config_file_update_times = "ois_curve_update_time_config.csv"
logging.info("Reading config file %s", config_file_update_times)
config_update_time = pd.read_csv(config_file_update_times)


#
# Load the required data from Bloomberg
#

tickers = config.Ticker.values

logging.info("Sending Bloomberg query for %s tickers", len(tickers))

target_date = datetime.now()

bbg_data = blp.bdh(tickers, "PX_LAST", target_date, target_date)

#
# Convert into tidy data and join with meta data from config
#

df_wide = bbg_data.reset_index().rename(columns={"index": "Date"})

df_tidy = df_wide.melt(id_vars = "Date", var_name="Ticker", value_name="Value")

df = df_tidy.merge(config, on = "Ticker")

#
# Check for missing tickers in the data
#

missing_data_tickers = set(tickers) - set(df["Ticker"])

if len(missing_data_tickers) > 0:
    logging.error("The following data points were not available: %s ", missing_data_tickers)

#
# Loop through all currencies and calculate the forwards
#

currencies = set(config.Currency.values)
current_hour = datetime.now().hour
frames = []

for ccy in currencies:
    
    c_df = df[df["Currency"] == ccy]
    
    # Update only if we are in the correct liquidity window
    first_update = config_update_time[config_update_time["Currency"] == ccy]["FirstUpdate"].to_numpy()[0]
    last_update = config_update_time[config_update_time["Currency"] == ccy]["LastUpdate"].to_numpy()[0]

    logging.debug("Checking for %s at %s hours, first update: %s, last_update: %s", ccy, current_hour, first_update, last_update)

    if current_hour >= first_update and current_hour < last_update:

        # Check that we can build a curve. If not, it may be a holiday for this currency
        na_count = c_df.groupby("Date")["Value"].apply(lambda x: x.isnull().sum())

        if na_count.values[0] > 0:
            logging.error("Found %s NA data points for %s", na_count, ccy)
        else:
            fwds = fwd_curve(rates=c_df["Value"].values, terms=c_df["Term"].values,
            eval_date=target_date, units=c_df["Unit"].values, currency=ccy)
            frames.append(fwds)

result = pd.concat(frames)

#
# Prefer strings for CSV output
#

result["Date"] = result["Date"].dt.strftime("%Y-%m-%d")
result["DateFwd"] = result["DateFwd"].dt.strftime("%Y-%m-%d")
result["Updated"] = result["Updated"].dt.strftime("%Y-%m-%d %H:%M:%S")

#
# We may have only updated for a subset of currencies, so carry
# over previous values for other currencies if need be
#

dated_file = "N:/Offdata/RM/_Data/Curves/ois_fwd/curves_" + target_date.strftime("%Y-%m-%d") + ".csv"

if os.path.exists(dated_file):    
    previous_rates = pd.read_csv(dated_file)
    logging.debug("Previous run found for today, loaded %s rows", len(previous_rates))
    logging.debug("Joining with collected %s rows", len(result))
    result = pd.concat([result, previous_rates])
    logging.debug("Concatenated frame has %s rows", len(result))
    result = result.groupby("Currency").apply(lambda x: x[x["Updated"] == max(x["Updated"])]).reset_index(drop = True)
    logging.debug("Final data frame of %s rows", len(result))

#
# Write out the final dataset
#

logging.debug("Writing to output csv...")
result.to_csv("N:/Offdata/RM/_Data/Curves/ois_fwd/curves.csv", index=False)

logging.debug("Writing dated copy of csv...")
result.to_csv(dated_file, index=False)

logging.info("Finished")
