import matplotlib.pyplot as plt
import yfinance as yf
import pandas as pd
import datetime
import os


# transaction payments column name
TRANSACTION_PAYMENT_COLUMN_NAME = "TRANSACTION_PAYMENT"

# fee payments column name
FEE_PAYMENT_COLUMN_NAME = "FEE_PAYMENT"

# portfolio column name
PORTFOLIO = "PORTFOLIO"

# portfolio data index name
DATE = "DATE"

# suffixes for columns with securities count, value, unit value, expense and profit
COUNT_SUFFIX = "_COUNT"
VALUE_SUFFIX = "_VALUE"
UNIT_VALUE_SUFFIX = "_UNIT_VALUE"
EXPENSE_SUFFIX = "_EXPENSE"
PROFIT_SUFFIX = "_PROFIT"
SINCE_INCEPTION_SUFFIX = "_SINCE_INCEPTION"
VALUE_AND_EXPENSE_SUFFIX = "_VALUE_AND_EXPENSE"
DRAWDOWN_SUFFIX = "_DRAWDOWN"


def download_yahoo(tickers, distinct_currencies, ohlc, analysis_currency, securities):
    """
    Downloads data from yahoo finance for tickers and currencies

    Parameters
    ----------
    tickers : list
        List of tickers to download
    distinct_currencies : str
        Currencies to download
    ohlc : str
        Open, High, Low, Close data to download
    analysis_currency : str
        Currency in which the analysis will be done
    securities : list
        List of securities names

    Returns
    -------
    DataFrame
        DataFrame with downloaded data for tickers
    DataFrame
        DataFrame with downloaded exchange rates for currencies
    """
    # convert ohlc to upper case first letter and lower case the rest
    ohlc = ohlc[0].upper() + ohlc[1:].lower()

    # download securities data from yahoo finance
    yahoo_securities_data = yf.download(tickers, period="max")[ohlc]
    df_securities = pd.DataFrame(yahoo_securities_data)

    # set columns order to a specified one in order to correctly set columns names later
    df_securities = df_securities[tickers]

    # set index name to DATE and set columns names to securities names
    df_securities.index.name = DATE
    df_securities.columns = securities

    # create list of currency pairs to download exchange rates for
    distinct_currency_pairs = [
        currency + analysis_currency for currency in distinct_currencies
    ]

    # remove currency pair which is the same as analysis currency
    distinct_currency_pairs.remove(analysis_currency * 2)

    # if currency pairs are specified then download exchange rates for them else set exchange rates to None
    distinct_currency_pairs_format = [
        currency + "=X" for currency in distinct_currency_pairs
    ]

    # download exchange rates from yahoo finance if there are any distinct currency pairs different than analysis currency
    # if distinct_currency_pairs_format:
    yahoo_currencies_data = yf.download(distinct_currency_pairs_format, period="max")[
        ohlc
    ]

    # check if yahoo_currencies_data is a Series or DataFrame
    if isinstance(yahoo_currencies_data, pd.Series):
        # create DataFrame with downloaded exchange rate and set column name to currency pair
        exchange_rates = pd.DataFrame(yahoo_currencies_data)
        exchange_rates.columns = distinct_currency_pairs
    else:
        # set columns order to a specified one in order to correctly set columns names later
        exchange_rates = yahoo_currencies_data[distinct_currency_pairs_format]
        exchange_rates.columns = distinct_currency_pairs

    # if there is analysis currency in distinct currencies then add column with exchange rates equal to 1.0
    if distinct_currencies.index(analysis_currency) != -1:
        analysis_currency_exchange_rates = pd.Series(
            [1.0 for _ in range(len(df_securities.index))],
            index=df_securities.index,
            name=analysis_currency * 2,
        )
        exchange_rates = pd.concat(
            [exchange_rates, analysis_currency_exchange_rates], axis=1
        )

    return df_securities, exchange_rates


def load_portfolio_transactions_data(
    portfolio_data_file_name,
    data_folder_path,
    exchange_rates,
    transaction_payment_list,
    fee_payment_list,
    analysis_currency,
):
    """
    Loads data from .csv file with portfolio transactions data

    Parameters
    ----------
    portfolio_data_file_name : str
        Name of the .csv file with portfolio data
    data_folder_path : str
        Path to folder with portfolio data files
    exchange_rates : DataFrame
        DataFrame with exchange rates
    transaction_payment_list : list
        List with transaction payment column as a first element and its currency as a second element
    fee_payment_list : list
        List with fee payment column as a first element and its currency as a second element
    analysis_currency : str
        Currency which will be used for analysis

    Returns
    -------
    DataFrame
        DataFrame with portfolio transactions data converted to analysis currency
    """
    portfolio_data = pd.read_csv(
        os.path.join(data_folder_path, portfolio_data_file_name),
        index_col=0,
        low_memory=False,
    )
    portfolio_data.index = pd.to_datetime(portfolio_data.index, format="%Y-%m-%d")
    portfolio_data.index.name = DATE

    # convert transaction and fee payments to analysis currency
    transaction_column_name = transaction_payment_list[0]
    fee_column_name = fee_payment_list[0]

    transaction_currency_pair = transaction_payment_list[1] + analysis_currency
    fee_currency_pair = fee_payment_list[1] + analysis_currency

    # convert transaction and fee payments to analysis currency and assign them to a new column
    # to resolve an issue that Pandas cannot reindex on an axis with duplicate labels I will do it in a loop
    for index in portfolio_data.index:
        portfolio_data.loc[index, TRANSACTION_PAYMENT_COLUMN_NAME] = (
            portfolio_data.loc[index, transaction_column_name]
            * exchange_rates.loc[index, transaction_currency_pair]
        )

        portfolio_data.loc[index, FEE_PAYMENT_COLUMN_NAME] = (
            portfolio_data.loc[index, fee_column_name]
            * exchange_rates.loc[index, fee_currency_pair]
        )

    # drop transaction and fee payments columns
    portfolio_data = portfolio_data.drop(
        [transaction_column_name, fee_column_name], axis=1
    )

    return portfolio_data


def prepare_portfolio_data(
    securities_data,
    exchange_rates,
    transaction_payments,
    fee_payments,
    analysis_currency,
    portfolio_data_files_names_and_payments_columns,
    data_folder_path,
    first_transaction_date,
):
    """
    Prepares portfolio data for analysis

    Parameters
    ----------
    securities_data : DataFrame
        DataFrame with securities data
    exchange_rates : DataFrame
        DataFrame with exchange rates
    transaction_payments : dict
        Dictionary with transaction payments columns and their currencies
    fee_payments : dict
        Dictionary with fee payments columns and their currencies
    analysis_currency : str
        Currency in which the analysis will be done
    portfolio_data_files_names_and_payments_columns : dict
        Dictionary with portfolio data files names and corresponding payments columns (buy/sold and fees)
    data_folder_path : str
        Path to folder with portfolio data files
    first_transaction_date : str
        First transaction date

    Returns
    -------
    DataFrame
        DataFrame with portfolio data prepared for analysis
    """
    # take only rows of DataFrame indexed from first_transaction_date
    securities_data = securities_data[
        securities_data.index
        >= datetime.datetime.strptime(first_transaction_date, "%Y-%m-%d")
    ].copy()

    # fill NaN values with previous values as we assume that if there is no value for a day it means that the stock market was closed that day and the value is the same as the previous day
    securities_data = securities_data.fillna(method="ffill")

    # just in case if there are still NaN values as the first rows of the DataFrame we fill them with 0
    securities_data = securities_data.fillna(0)

    # load portfolio data from .csv files where dates, securities and values of transactions are stored and concatenate them into one DataFrame
    portfolio_data = pd.DataFrame()
    for (
        portfolio_data_file_name,
        payment_columns,
    ) in portfolio_data_files_names_and_payments_columns.items():
        # take column which specifies the transaction payment currency, find a currency and create a list with column name and currency
        transaction_column = payment_columns.get(TRANSACTION_PAYMENT_COLUMN_NAME)
        transaction_currency = transaction_payments.get(transaction_column)
        transaction_payment_list = [transaction_column, transaction_currency]

        # take column which specifies the fee payment currency, find a currency and create a list with column name and currency
        fee_column = payment_columns.get(FEE_PAYMENT_COLUMN_NAME)
        fee_currency = fee_payments.get(fee_column)
        fee_payment_list = [fee_column, fee_currency]

        # load part of portfolio data from .csv file
        portfolio_data_part = load_portfolio_transactions_data(
            portfolio_data_file_name,
            data_folder_path,
            exchange_rates,
            transaction_payment_list,
            fee_payment_list,
            analysis_currency,
        )

        # concatenate part of portfolio data with the rest of portfolio data
        portfolio_data = pd.concat([portfolio_data, portfolio_data_part])

    # merge raw securities data with portfolio data
    portfolio_data = securities_data.join(portfolio_data, rsuffix=COUNT_SUFFIX)

    # sort index in ascending order to ensure that the dates are in the desired order
    portfolio_data = portfolio_data.sort_index()

    return portfolio_data


def calculate_portfolio_values(
    portfolio_data,
    securities,
):
    """
    Calculates portfolio components' property values for each security and portfolio as a whole and adds them to portfolio_data DataFrame

    Parameters
    ----------
    portfolio_data : DataFrame
        DataFrame with portfolio data
    securities : list
        List of securities names

    Returns
    -------
    DataFrame
        DataFrame with portfolio data with calculated values
    """
    # list of columns for portfolio different values for each security
    securities_count = [col + COUNT_SUFFIX for col in securities]
    securities_value = [col + VALUE_SUFFIX for col in securities]
    securities_unit_value = [col + UNIT_VALUE_SUFFIX for col in securities]
    securities_expense = [col + EXPENSE_SUFFIX for col in securities]
    securities_profit = [col + PROFIT_SUFFIX for col in securities]

    # assign values from portfolio_data_columns_count to portfolio_data_columns_value and portfolio_data_columns_expense as values and expenses will be calculated later using these count values
    portfolio_data[securities_value] = portfolio_data[securities_count]
    portfolio_data[securities_expense] = portfolio_data[securities_count]

    # rename columns to more informative names
    portfolio_data = portfolio_data.rename(
        columns=dict(zip(securities, securities_unit_value))
    )

    # temporarily reset index to get rid of duplicate index values
    portfolio_data = portfolio_data.reset_index()

    # security expense without transaction fee
    for security_expense in securities_expense:
        portfolio_data[security_expense] = portfolio_data[
            portfolio_data[security_expense] > 0
        ][TRANSACTION_PAYMENT_COLUMN_NAME]

    # set index back to DATE
    portfolio_data = portfolio_data.set_index(DATE)

    # separate securities unit values from other columns and drop duplicates in DATE column from these separated securities unit values
    duplicated_idx = portfolio_data.index.duplicated()
    unit_values_data = portfolio_data.loc[~duplicated_idx, securities_unit_value].copy()

    # drop securities unit values from portfolio_data DataFrame
    portfolio_data = portfolio_data.drop(securities_unit_value, axis=1)

    # group by DATE and sum all the values for each DATE
    portfolio_data = portfolio_data.groupby(DATE).sum()

    # join portfolio_data DataFrame with separated earlier securities unit values
    portfolio_data = portfolio_data.join(unit_values_data)

    # currently in securities values there is only count of securities securities as an auxiliary column to calculate portfolio values later
    # we fill NaN values with 0 and calculate cummulative sum to get the number of securities in the portfolio at a given time
    portfolio_data[securities_value] = (
        portfolio_data[securities_value].fillna(0).cumsum()
    )

    # calculate portfolio values for each security using currently stored count of securities and unit value
    for security_value, security_unit_value in zip(
        securities_value, securities_unit_value
    ):
        portfolio_data[security_value] = (
            portfolio_data[security_value] * portfolio_data[security_unit_value]
        )

    # fill all the remaining NaN values with 0
    portfolio_data = portfolio_data.fillna(0)

    # calculate cummulative sum of expenses and count of securities
    portfolio_data[securities_expense] = portfolio_data[securities_expense].cumsum()
    portfolio_data[securities_count] = portfolio_data[securities_count].cumsum()

    # add fees to transaction payments
    portfolio_data[TRANSACTION_PAYMENT_COLUMN_NAME] = (
        portfolio_data[TRANSACTION_PAYMENT_COLUMN_NAME]
        + portfolio_data[FEE_PAYMENT_COLUMN_NAME]
    )

    # calculate cummulative sum of portfolio expenses as a sum of transaction payments and fees
    portfolio_data[PORTFOLIO + EXPENSE_SUFFIX] = portfolio_data[
        TRANSACTION_PAYMENT_COLUMN_NAME
    ].cumsum()

    # calculate portfolio value as a sum of securities values
    portfolio_data[PORTFOLIO + VALUE_SUFFIX] = portfolio_data[securities_value].sum(
        axis=1
    )

    # calculate portfolio profit as a difference between portfolio value and portfolio expense
    portfolio_data[PORTFOLIO + PROFIT_SUFFIX] = (
        portfolio_data[PORTFOLIO + VALUE_SUFFIX]
        - portfolio_data[PORTFOLIO + EXPENSE_SUFFIX]
    )

    # calculate profit for each security as a difference between security value and security expense
    for security_value, security_expense, security_profit in zip(
        securities_value, securities_expense, securities_profit
    ):
        portfolio_data[security_profit] = (
            portfolio_data[security_value] - portfolio_data[security_expense]
        )

    # calculate portfolio drawdowns
    for date in portfolio_data.index:
        max_value = portfolio_data.loc[:date, PORTFOLIO + VALUE_SUFFIX].max()
        current_value = portfolio_data.loc[date, PORTFOLIO + VALUE_SUFFIX]

        if max_value == 0:
            portfolio_data.loc[date, PORTFOLIO + DRAWDOWN_SUFFIX] = 0
            continue

        portfolio_data.loc[date, PORTFOLIO + DRAWDOWN_SUFFIX] = (
            current_value - max_value
        ) / max_value

    # concatenate columns to leave into one list
    columns_to_leave = [
        securities_count
        + securities_value
        + securities_unit_value
        + securities_expense
        + securities_profit
        + [
            PORTFOLIO + VALUE_SUFFIX,
            PORTFOLIO + EXPENSE_SUFFIX,
            PORTFOLIO + PROFIT_SUFFIX,
            PORTFOLIO + DRAWDOWN_SUFFIX,
        ]
    ]

    # leave only specified columns
    portfolio_data = portfolio_data[columns_to_leave[0]]

    return portfolio_data


def calculate_current_status(portfolio_data_calculations, weight_groups, securities):
    """
    Calculates current status of the portfolio

    Parameters
    ----------
    portfolio_data_calculations : DataFrame
        DataFrame with portfolio data calculations
    weight_groups : dict
        Dictionary with weights groups
    securities : list
        List of securities names

    Returns
    -------
    DataFrame
        DataFrame with current status of the portfolio
    """
    portfolio_current_data = portfolio_data_calculations.iloc[-1].copy()

    securities_value = [col + VALUE_SUFFIX for col in securities]

    # for storing current value for each weights group
    weight_groups_current_values = {}

    # calculate current values for each weights group
    for security_name, security_value in zip(securities, securities_value):
        security_current_value = portfolio_current_data[security_value]
        weight_group = [
            key for key, value in weight_groups.items() if security_name in value
        ][0]
        weight_groups_current_values.update(
            {
                weight_group: security_current_value
                + weight_groups_current_values.get(weight_group, 0)
            }
        )

    for (
        weight_group,
        weight_group_current_value,
    ) in weight_groups_current_values.items():
        portfolio_current_data[weight_group + VALUE_SUFFIX] = weight_group_current_value

    return portfolio_current_data
