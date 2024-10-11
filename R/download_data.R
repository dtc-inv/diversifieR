# sec and BD holdings ----

#' @title Download Holdings from SEC Database
#' @param long_cik string to represent the fund / share-class CIK,
#'   typically longer in length
#' @param short_cik string to represent the parent company CIK,
#'   typically shorter in length
#' @param user_email SEC's API requires an email address
#' @param as_of optional date to mark when the download occurred
#' @return data.frame with fund holdings
#' @export
download_sec_nport = function(long_cik, short_cik, user_email) {
  doc_type <- 'nport-p'
  url <- paste0(
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=',
    long_cik,
    '&type=',
    doc_type,
    '&dateb=&count=5&scd=filings&search_text=')
  # SEC requires email as a header for authentication, you don't have to register
  # email on their site, just need valid email address
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  html_doc <- rvest::read_html(response)
  tbl <- rvest::html_table(html_doc)
  # might need to change which table to target if web design changes
  long_str <- tbl[[3]]$Description[1]
  file_date <- tbl[[3]]$`Filing Date`[1]
  # need to extract ##-##-## from table cell to target the url of the latest filing
  num_id <- stringr::str_extract(long_str, '[[:digit:]]+-[[:digit:]]+-[[:digit:]]+')
  num_str <- gsub('-', '', num_id)
  # now we can target url of latest filing and get the holdings
  url <- paste0('https://www.sec.gov/Archives/edgar/data/',
                short_cik, '/', num_str, '/primary_doc.xml')
  response <- httr::GET(url, add_headers(`User-Agent` = user_email))
  xml_file <- xml2::read_xml(response)
  doc <- XML::xmlParse(xml_file)
  root <- XML::xmlRoot(doc)
  # xmlToList can take ~10+ seconds for large amount of holdings, is there more efficient way?
  all_list <- XML::xmlToList(root)
  # current node is invstOrSecs for the holdings
  all_sec <- all_list$formData$invstOrSecs
  df <- data.frame(
    name = NA,
    lei = NA,
    title = NA,
    cusip = NA,
    isin = NA, # conditional, required to list at least one additional ID
    altid = NA, # ID is usually ISIN, check for ISIN otherwise call alt
    balance = NA,
    units = NA,
    curCd = NA,
    exchangeRt = NA, # conditional, if no FX then this field doesn't exist
    valUSD = NA,
    pctVal = NA,
    payoffProfile = NA,
    assetCat = NA,
    issuerCat = NA,
    invCountry = NA,
    returnInfo = NA
  )
  for (i in 1:length(all_sec)) {
    x <- all_sec[[i]]
    df[i, 'name'] <- x$name
    df[i, 'lei'] <- x$lei
    df[i, 'title'] <- x$title
    df[i, 'cusip']  <- x$cusip
    if ('isin' %in% names(x$identifiers)) {
      df[i, 'isin'] <- x$identifiers$isin
      df[i, 'altid'] <- NA
    } else {
      df[i, 'isin'] <- NA
      df[i, 'altid'] <- x$identifiers[[1]][2]
    }
    df[i, 'balance'] <- x$balance
    df[i, 'units'] <- x$units
    if ('curCd' %in% names(x)) {
      df[i, 'curCd'] <- x$curCd
      df[i, 'exchangeRt'] <- NA
    } else if ('currencyConditional' %in% names(x)) {
      df[i, 'curCd'] <- x$currencyConditional[1]
      df[i, 'exchangeRt'] <- x$currencyConditional[2]
    } else {
      df[i, 'curCd'] <- NA
      df[i, 'exchangeRt'] <- NA
    }
    df[i, 'valUSD'] <- x$valUSD
    df[i, 'pctVal'] <- x$pctVal
    df[i, 'payoffProfile'] <- x$payoffProfile
    if ('assetCat' %in% names(x)) {
      df[i, 'assetCat'] <- x$assetCat
    } else if ('assetConditional' %in% names(x)) {
      df[i, 'assetCat'] <- x$assetConditional[2]
    } else {
      df[i, 'assetCat'] <- NA
    }
    if ('issuerCat' %in% names(x)) {
      df[i, 'issuerCat'] <- x$issuerCat[1]
    } else {
      df[i, 'issuerCat'] <- NA
    }
    df[i, 'invCountry'] <- x$invCountry
  }
  df[ ,'returnInfo'] <- as.Date(file_date)
  df$pctVal <- as.numeric(df$pctVal)
  df$pctVal <- df$pctVal / 100
  return(df)
}


#' @title Download Holdings from BlackDiamond
#' @param account_id string to represent target account (internal ID)
#' @param as_of date to pull holdings as of, latest is last business day
#' @return data.frame with holdings
#' @export
download_bd <- function(account_id, api_keys, as_of = NULL) {
  if (is.null(as_of)) {
    as_of <- last_us_trading_day()
  }
  bd_key <- api_keys$bd_key
  response <- httr::POST(
    'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
    accept_json(),
    add_headers(
      Authorization = paste0('Bearer ', bd_key$refresh_token),
      `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
    ),
    encode = 'json',
    body = list(accountID = account_id, asOf = as_of,
                onlyCurrentHoldings = TRUE, limit = 100000,
                include = list(returnInfo = TRUE, assets = TRUE))
  ) # end POST
  if (response$status_code != 200) {
    bd_key <- refresh_bd_key(api_keys, TRUE)
    response <- httr::POST(
      'https://api.blackdiamondwealthplatform.com/account/Query/HoldingDetailSearch',
      accept_json(),
      add_headers(
        Authorization = paste0('Bearer ', bd_key$refresh_token),
        `Ocp-Apim-Subscription-Key` = bd_key$bd_subkey
      ),
      encode = 'json',
      body = list(accountID = account_id, asOf = as_of,
                  onlyCurrentHoldings = TRUE, limit = 100000,
                  include = list(returnInfo = TRUE, assets = TRUE))
    ) # end POST
  }
  # parse jason and extract data
  rd <- parse_json(response)
  rd <- rd$data
  if (length(rd) == 0) {
    warning('no data found')
    return(NULL)
  }
  # convert json list into data.frame by looping through each holding
  df <- data.frame(
    assetId = NA,
    name = NA,
    cusip = NA,
    ticker = NA,
    identifier = NA,
    units = NA,
    emv = NA,
    returnInfo = NA
  )
  robcheck <- function(x, l) {
    if (x %in% names(l)) {
      if (!is.null(l[[x]])) {
        return(l[x])
      } else {
        return(NA)
      }
    } else {
      return(NA)
    }
  }
  for (i in 1:length(rd)) {
    df[i, 'assetId'] <- rd[[i]]$asset['assetId'][[1]]
    df[i, 'name'] <- rd[[i]]$asset['name'][[1]]
    df[i, 'cusip'] <- robcheck('cusip', rd[[i]]$asset)
    df[i, 'ticker'] <- robcheck('ticker', rd[[i]]$asset)
    df[i, 'identifier'] <- robcheck('identifier', rd[[i]]$asset)
    df[i, 'units'] <- rd[[i]]$returnInfo['units'][[1]]
    df[i, 'emv'] <- rd[[i]]$returnInfo['emv'][[1]]
    df[i, 'returnInfo'] <- rd[[i]]$returnInfo['returnDate'][[1]]
  }
  df$pctVal <- df$emv / sum(df$emv, na.rm = TRUE)
  return(df)
}


#' @export
refresh_bd_key = function(api_keys, save_to_n = FALSE) {
  bd_key <- api_keys$bd_key
  response <- httr::POST('https://login.bdreporting.com/connect/token',
                         encode = 'form',
                         body = list(grant_type = 'password',
                                     client_id = bd_key$client_id,
                                     client_secret = bd_key$client_secret,
                                     username = bd_key$username,
                                     password = bd_key$password))
  tk <- jsonlite::parse_json(response)
  bd_key$refresh_token <- tk$access_token
  if (save_to_n) {
    api_keys$bd_key <- bd_key
    save(api_keys, file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData')
  }
  return(bd_key)
}

# factset ----

#' @title Download from Factset Global Prices API
#' @export
download_fs_gp <- function(api_keys, ids, date_start, date_end, freq = 'D') {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  url <- "https://api.factset.com/content/factset-global-prices/v1/returns"
  request <- list(
    ids = as.list(ids),
    startDate = date_start,
    endDate = date_end,
    frequency = freq,
    dividendAdjust = "EXDATE_C",
    batch = "N"
  )
  response <- httr::POST(
    url, authenticate(username, password), body = request,
    add_headers(Accept = 'application/json'), encode = 'json')
  output <- rawToChar(response$content)
  json <- parse_json(output)
  return(json)
}

#' @title Flatten JSON returned from Factset Global API
#' @export
flatten_fs_gp <- function(json) {
  if ('status' %in% names(json)) {
    if (json$status == "Bad Request") {
      warning('bad request, returning empty data.frame')
      return(data.frame())
    }
  }
  dat <- json$data
  requestId <- sapply(dat, '[[', 'requestId')
  if (is.list(requestId)) {
    requestId <- unlist(list_replace_null(requestId))
  }
  date <- sapply(dat, '[[', 'date')
  if (is.list(date)) {
    date <- unlist(list_replace_null(date))
  }
  totalReturn <- sapply(dat, '[[', 'totalReturn')
  if (is.list(totalReturn)) {
    totalReturn <- unlist(list_replace_null(totalReturn))
  }
  df <- data.frame(
    requestId = requestId,
    date = date,
    totalReturn = totalReturn
  )
  return(df)
}


#' @export
download_fs_exchange_ret <- function(
    ids,
    api_keys,
    date_start = NULL,
    date_end = NULL)
{
  res <- list()
  if (length(ids) > 50) {
    iter <- seq(1, length(ids), 49)
    if (iter[length(iter)] < length(ids)) {
      iter <- c(iter, length(ids))
    }
  } else {
    iter <- c(1, length(ids))
  }
  res$ids <- ids
  res$iter <- iter

  if (is.null(date_start)) {
    date_start <- as.Date('2006-01-03')
  }
  if (is.null(date_end)) {
    date_end <- Sys.Date() - 1
  }
  df <- data.frame()
  for (i in 1:(length(res$iter)-1)) {
    json <- download_fs_gp(
      api_keys = api_keys,
      ids = res$ids[res$iter[i]:res$iter[i+1]],
      date_start = date_start,
      date_end = date_end,
      freq = 'D'
    )
    df <- rob_rbind(df, flatten_fs_gp(json))
    print(res$iter[i])
  }
  if (nrow(df) == 0) {
    warning('no returns found, returning json to inspect')
    return(json)
  }
  df$totalReturn <- df$totalReturn / 100
  is_dup <- duplicated(paste0(df$requestId, df$date))
  if (any(is_dup)) {
    warning('duplicated returns found, removing with duplicated check')
  }
  df <- df[!is_dup, ]
  df$date <- as.Date(df$date)
  wdf <- pivot_wider(df, id_cols = date, values_from = totalReturn,
                     names_from = requestId)
  dataframe_to_xts(wdf)
}

#' @export
download_fs_mutual_fund_ret <- function(ids, api_keys, days_back) {
  res <- list()
  if (length(ids) > 50) {
    iter <- seq(1, length(ids), 49)
    if (iter[length(iter)] < length(ids)) {
      iter <- c(iter, length(ids))
    }
  } else {
    iter <- c(1, length(ids))
  }
  res$ids <- ids
  res$iter <- iter
  ret_df <- data.frame()
  formulas <- paste0('P_TOTAL_RETURNC(-', days_back, 'D,NOW,D,USD)')
  for (i in 1:(length(iter)-1)) {
    xids <- ids[iter[i]:iter[i+1]]
    json <- download_fs(
      api_keys = api_keys,
      ids = xids,
      formulas = formulas,
      type = 'ts'
    )
    dat <- json$data
    res <- lapply(dat, '[[', formulas)
    res <- list_replace_null(res)
    res <- unlist(res)
    dt <- sapply(dat, '[[', 'date')
    req_id <- sapply(dat, '[[', 'requestId')
    i_df <- data.frame(requestId = req_id, date = dt, value = res)
    ret_df <- rob_rbind(ret_df, i_df)
  }
  if (nrow(ret_df) == 0) {
    warning('no returns found, returning json to inspect')
    return(json)
  }
  ret_df$value <- ret_df$value / 100
  is_dup <- duplicated(paste0(ret_df$requestId, ret_df$date))
  if (any(is_dup)) {
    warning('duplicated values found, removing with duplicated function')
  }
  ret_df <- ret_df[!is_dup, ]
  wdf <- pivot_wider(ret_df, id_cols = date, values_from = value,
                     names_from = requestId)
  dataframe_to_xts(wdf)
}

#' @title Download Daily Returns from Factset
#' @param id factset ID, e.g., "CLIENT:/PA_SOURCED_RETURNS/DTC_US_ACTIVE_EQUITY_COMMON_FUND"
#' @param api_keys list with API keys
#' @param t_minus integer for how many days back to start download
#' @return list with id and xts of returns
#' @export
download_fs_ctf_ret <- function(id, api_keys, t_minus = 5, freq = 'D') {
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  base_url <- 'https://api.factset.com/formula-api/v1/time-series?ids=$IDS&formulas='
  request <- paste0(
    base_url,
    'RA_RET(',
    "\"",
    id,
    "\",-",
    t_minus,
    "/0/0,0,", freq,
    ",FIVEDAY,USD,1)&flatten=Y"
  )
  response <- httr::GET(request, authenticate(username, password))
  output <- rawToChar(response$content)
  dat <- parse_json(output)
  dat <- dat[[1]]
  dt <- lapply(dat, '[[', 'date')
  dt <- list_replace_null(dt)
  val <- lapply(dat, '[[', 2)
  val <- list_replace_null(val)
  res <- xts(unlist(val) / 100, as.Date(unlist(dt)))
  colnames(res) <- id
  return(res)
}

#' @title Factset Formula API
#' @param api_keys list with API keys
#' @param ids string vector with ids, e.g., ticker, CUSIP, ISIN
#' @param formulas string vector with formulas, see details
#' @param features string vector describing formula results, see details
#' @return data.frame with data
#' @details Formulas are factset API formulas, e.g., "P_PRICE(0, -2, D)". You
#'   can list multiple formulas as strings:
#'   c("P_PRICE(0, -2, D)", "FF_EPS(QTR_R, 0)"). Features are a description to
#'   match results of formulas, in this example c("Price", "EPS").
#' @export
download_fs <- function(api_keys, ids, formulas, type = c('ts', 'cs'),
                        flatn = TRUE) {
  if (type[1] == 'ts') {
    struc <- 'time-series'
  } else {
    struc <- 'cross-sectional'
  }
  username <- api_keys$fs$username
  password <- api_keys$fs$password
  ids[is.na(ids)] <- ""
  request <- paste0(
    "https://api.factset.com/formula-api/v1/",
    struc,
    "?ids=",
    paste0(ids, collapse = ","),
    "&formulas=",
    paste0(formulas, collapse = ",")
  )
  if (flatn) {
    request <- paste0(request, '&flatten=Y')
  }
  response <- httr::GET(request, authenticate(username, password))
  print(response$status)
  output <- rawToChar(response$content)
  json <- parse_json(output)
  return(json)
}


# HFR ----

#' @title Read HFR ROR csv file
#' @param wb file path of csv
#' @param min_track length (months) of minimum track record, set to 0 to not
#'   exclude any funds
#' @return tibble of returns in tidy longer format to preserve meta-data
#' @export
read_hfr_csv <- function(wb, min_track = 30) {
  raw <- read.csv(wb)
  if (min_track > 0) {
    bad_row <- rowSums(!is.na(raw[, 8:ncol(raw)])) < min_track
    dat <- raw[!bad_row, ]
  } else {
    dat <- raw
  }
  res <- tidyr::pivot_longer(dat, starts_with('X'), names_to = 'Date',
                             values_to = 'Value')
  dt <- gsub('X', '', res$Date)
  dt <- ymd(paste0(dt, '.01'))
  dt <- ceiling_date(dt, 'months') - 1
  res$Date <- dt
  res$Value <- res$Value / 100
  return(res)
}


#' @title Save HFR ROR data to S3
#' @param bucket s3 bucket from arrow
#' @param s3_file_name name to save in s3
#' @param wb optional file path to load csv file
#' @param dat optional data.frame or tibble to upload (instead of reading csv)
#' @return nothing is returned, if successful the file will be uploaded to S3
#' @export
hfr_csv_to_s3 <- function(bucket, s3_file_name, wb = NULL, dat = NULL) {
  if (!is.null(wb)) {
    dat <- read_hfr_csv(wb)
  }
  if (is.null(dat)) {
    stop('must provide either wb or dat')
  }
  write_parquet(dat, bucket$path(paste0('hfr-files/', s3_file_name, '.parquet')))
}


#' @export
hfr_csv_to_xts <- function(df) {
  r <- tidyr::pivot_wider(df[, c('FUND_NAME', 'Date', 'Value')],
                          names_from = FUND_NAME, values_from = Value)
  r <- xts(r[, -1], as.Date(r[[1]]))
  return(r)
}


#' @title Read HFR ASCI file and return a list with data.tables of returns
#' and meta-data
#' @param fpath folder location of downloaded files
#' @export
read_hfr_asci <- function(fpath) {
  ror <- read.table(paste0(fpath, 'ASCII_Ror.txt'), sep = ',', header = TRUE)
  ror$Performance <- ror$Performance / 100
  ror$Date <- as.Date(ror$Date, format = '%m/%d/%Y')
  meta <- read.table(paste0(fpath, 'ASCII_Fund.txt'), sep = ',', header = TRUE)
  res <- list()
  res$ror <- ror
  res$meta <- meta
  return(res)
}


#' @title Read HFR ASCI file and save to S3
#' @param fpath file path location of saved ASCI files
#' @param bucket S3 File System from Database
#' @param s3_file_name prefix of name to save, e.g., "hfri-all"
#' @details
#' Download the ASCI file from the HFRI database download section
#' The fpath will contain multiple files, the returns and meta data file
#' will be read in and saved. The s3_file_name parameter will determine the names,
#' e.g., "hfri-all" will save "hfri-all-ror.parquet" and "hfri-all-meta.parquet"
#' to S3.
#' @export
hfr_asci_to_s3 <- function(fpath, bucket, s3_file_name) {
  dat <- read_hfr_asci(fpath)
  write_parquet(dat$ror, bucket$path(
    paste0('hfr-files/', s3_file_name, '-ror', '.parquet')))
  write_parquet(dat$meta, bucket$path(
    paste0('hfr-files/', s3_file_name, '-meta', '.parquet')))
}







# tiingo ----

#' @title Download price time-series from Tiingo
#' @param ticker character of stock ticker to download, see
#'   `download_tiingo_tickers` for downloading multiple tickers
#' @param t_api API key
#' @param date_start inception for time-series
#' @param date_end last day of time-series, if left `NULL` will default to today
#' @return `data.frame` of historical prices (adjusted for splits and dividends)
#'   with dates in first column
#' @export
download_tiingo_csv <- function(ticker, t_api, date_start = '1970-01-01',
                                date_end = NULL) {
  if (is.null(date_end)) {
    date_end <- Sys.Date()
  }
  t_url <- paste0('https://api.tiingo.com/tiingo/daily/',
                  ticker,
                  '/prices?startDate=', date_start,
                  '&endDate=', date_end,
                  '&format=csv&resampleFreq=daily',
                  '&token=', t_api)
  dat <- try(read.csv(t_url), silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' was not downloaded'))
    return(NULL)
  }
  print(paste0(ticker, ' downloaded'))
  dat <- try(dat[, c('date', 'adjClose')], silent = TRUE)
  if ('try-error' %in% class(dat)) {
    warning(paste0(ticker, ' columns not found'))
    return(NULL)
  }
  colnames(dat)[2] <- ticker
  return(dat)
}


#' @title Download multiple tickers from Tiingo
#' @param ticker_vec character vector of tickers to download
#' @param t_api API key
#' @param date_start first date in time-series to download, need to be Date class,
#'   e.g., as.Date('2024-01-01')
#' @param date_end last date in time-series to download, if left `NULL` will
#'   default to today
#' @param out_ret boolean if TRUE outputs returns, if FALSE outputs prices
#' @param out_xts boolean, if TRUE outputs xts object, if FALSE returns data.frame
#' @return xts of price time-series adjusted for dividends and splits
#' @export
download_tiingo_tickers <- function(ticker_vec, t_api, date_start = NULL,
                                    date_end = NULL, out_ret = FALSE,
                                    out_xts = FALSE) {

  if (is.null(date_start)) {
    date_start <- as.Date('1970-01-01')
  }
  utick <- unique(ticker_vec)
  if (length(utick) < length(ticker_vec)) {
    warning('duplicated tickers found and removed')
  }
  dat <- lapply(utick, download_tiingo_csv, t_api = t_api,
                date_start = date_start, date_end = date_end)
  dat <- dat[!sapply(dat, is.null)]
  if (length(dat) == 0) {
    warning('no tickers found')
    return()
  }
  nm <- sapply(dat, function(x) {colnames(x)[2]})
  if (length(dat) == 1) {
    price <- dat[[1]][, 2]
    dt <- dat[[1]][, 1]
  } else {
    dt <- us_trading_days(date_start, date_end)
    price <- matrix(nrow = length(dt), ncol = length(dat))
    for (i in 1:ncol(price)) {
      ix <- match(dat[[i]]$date, dt)
      price_on_holiday <- is.na(ix)
      price[na.omit(ix), i] <- dat[[i]][[2]][!price_on_holiday]
      if (i %% 100 == 0) print(paste0(i, ' out of ', length(dat)))
    }
  }
  price_df <- data.frame('date' = dt, price)
  colnames(price_df) <- c('date', nm)
  if (out_ret) {
    res <- mat_to_xts(price_df)
    res <- price_to_ret(res)
    colnames(res) <- nm
    if (!out_xts) {
      res <- xts_to_dataframe(res)
    }
  } else {
    res <- price_df
    if (out_xts) {
      res <- mat_to_xts(price_df)
      colnames(res) <- nm
    }
  }
  return(res)
}

# fred ----

#' @title Download Econ Time-series from FED Database
#' @param series_id character to represent the series you want to download
#' @param fred_api API key, see https://fred.stlouisfed.org/ to create one
#' @return xts of economic time-series
#' @export
download_fred <- function(series_id, fred_api) {
  f_url <- paste0('https://api.stlouisfed.org/fred/series/observations?series_id=',
                  series_id, '&api_key=', fred_api, '&file_type=json')
  request <- httr::GET(f_url)
  dat <- jsonlite::parse_json(request)
  dt <- sapply(dat$observations, '[[', 'date')
  obs <- sapply(dat$observations, '[[', 'value')
  x <- xts(as.numeric(obs), as.Date(dt))
  colnames(x) <- series_id
  return(x)
}

