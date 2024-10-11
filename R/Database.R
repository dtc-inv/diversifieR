#' Database Object
#'
#' @description
#' Database connects to various data providers, e.g., FactSet, Black Diamond,
#'   SEC EDGAR, etc, to pull data and organize in tables files (arrow, parquet)
#'   in AWS S3.
#' @export
Database <- R6::R6Class(
  'Database',
  public = list(

    #' @field msl table: master security list
    msl = NULL,

    #' @field api_keys optional list containing api_keys
    api_keys = NULL,

    #' @field bucket `S3FileSystem` object from `arrow`
    bucket = NULL,

    #' @field macro list with tables for R3000 and ACWI model from Piper Sandler
    macro = NULL,

    #' @field ret list with daily and monthly return tables
    ret = NULL,

    #' @field fina table with latest financial metrics for companies
    fina = NULL,

    #' @description Create a Database
    #' @param msl table: master security list
    #' @param geo table: country number and geography
    #' @param bucket `S3FileSystem` object from `arrow`
    #' @param api_keys list of api_keys
    #' @param api_file `.RData` file to load api_key list
    #' @details
    #' All fields are optional, except the api keys must be either entered
    #' as a list through the api_keys parameter or a file path to load the
    #' api keys via api_file. The tables are data.frames or tibbles stored
    #' as arrow or parquet files in S3, accessed through the bucket
    #' S3FileSystem.
    initialize = function(
    msl = NULL,
    bucket = NULL,
    api_keys = NULL,
    api_file = 'N:/Investment Team/DATABASES/MDB/Keys/api_keys.RData')
    {
      if (is.null(api_keys)) {
        if (file.exists(api_file)) {
          load(api_file)
          if (exists('api_keys')) {
            self$api_keys <- api_keys
          } else {
            stop('api keys list is not named api_keys')
          }
        } else {
          if (is.null(api_keys)) {
            stop('must either supply api_keys or api_file location')
          } else {
            self$api_keys <- api_keys
          }
        }
      }
      self$check_api_keys()
      bucket <- arrow::s3_bucket(
        'dtc-inv',
        access_key = api_keys$s3$access_key,
        secret_key = api_keys$s3$secret_key
      )
      self$bucket <- bucket
      self$msl <- msl
      self$check_msl()
      self$bucket <- bucket
    },

    # tables -----

    #' @description save master security list in S3
    #' @param msl master security list
    #' @details
    #' If msl is left as NULL the msl stored in the Database Object as will
    #' be written. If a data.frame or tibble is entered then that will
    #' table will be saved to S3. Note there is only one msl, whatever is
    #' saved will overwrite the table in S3.
    write_msl = function(msl = NULL) {
      if (is.null(msl)) {
        msl <- self$msl
      }
      if (is.null(msl)) {
        stop('msl is missing')
      }
      if (!'data.frame' %in% class(msl)) {
        stop(paste0('msl is wrong structure ', class(msl)))
      }
      self$check_bucket()
      write_parquet(msl, self$bucket$path('tables/msl.parquet'))
    },

    #' @description Read msl from S3
    read_msl = function() {
      self$check_bucket()
      self$msl <- read_parquet(self$bucket$path('tables/msl.parquet'))
    },

    #' @description Read msl from Excel
    #' @param path file path where workbook is stored
    read_msl_xl = function(
    path = 'N:/Investment Team/DATABASES/MDB/Tables/msl.xlsx') {
      self$msl <- readxl::read_excel(
        path = path,
        sheet = 'msl',
        col_types = c('text', 'text', 'numeric', 'numeric',
                      rep('text', 13), 'numeric')
      )
    },

    #' @description Read macro workbooks from S3
    read_macro = function() {
      self$check_bucket()
      r3 <- read_parquet(self$bucket$path('macro/macro_r3.parquet'))
      acwi <- read_parquet(self$bucket$path('macro/macro_acwi.parquet'))
      self$macro <- list(r3 = r3, acwi = acwi)
    },

    # checks ----

    #' @description Check if bucket is not NULL
    check_bucket = function() {
      if (is.null(self$bucket)) {
        stop('bucket is missing')
      }
    },

    #' @description
    #' Check if MSL is loaded and has unique DTCNames (key id).
    #'   Will try from S3 if NULL.
    check_msl = function() {
      if (is.null(self$msl)) {
        x <- try(self$read_msl())
        if ('data.frame' %in% class(x)) {
          if (sum(duplicated(x$DTCName))) {
            warning('duplicated DTCNames found')
          }
          return('pass')
        } else {
          stop('msl is missing and could not auto load')
        }
      } else {
        if ('data.frame' %in% class(self$msl)) {
          return('pass')
          if (sum(duplicated(x$DTCName))) {
            warning('duplicated DTCNames found')
          }
        } else {
          stop('msl is not a data.frame')
        }
      }
    },

    #' @description
    #' Check if api_keys are properly structured as named list. Will need
    #' to update if more keys are added.
    check_api_keys = function() {
      if (is.null(self$api_keys)) {
        stop('api keys are missing')
      }
      nm_check <- c('t_api', 'bd_key', 's3') %in% names(self$api_keys)
      if (!all(nm_check)) {
        stop('names of api_keys list not properly specified')
      }
    },

    # holdings ----

    #' @title Get historical holdings from black diamond over multiple months
    #' @param dtc_name DTCName in MSL of portfolio / account
    #' @param date_start first (oldest) date to start holdings download
    #' @param date_end most recent date to end holdings download period
    #' @details
    #' Holdings each month between the start and end dates will be downloaded
    #' and saved to AWS.
    back_fill_bd_holdings = function(dtc_name, date_start, date_end) {
      obs <- subset_df(self$msl, "DTCName", dtc_name)
      if (nrow(obs) == 0) {
        warning(paste0(dtc_name, " not found"))
        return(NULL)
      }
      date_start <- try(as.Date(date_start))
      date_end <- try(as.Date(date_end))
      if ("try-error" %in% class(date_start) | "try-error" %in% class(date)) {
        return("date input error, could not convert with as.Date()")
      }
      xmon <- lubridate::ceiling_date(seq.Date(
        date_start,
        date_end,
        by = freq))
      xmon <- as.Date(xmon) - 1
      bizdays::create.calendar('cal', holidays =
                                 timeDate::holidayNYSE(1900:2100),
                               weekdays = c('saturday', 'sunday'))
      date_seq <- bizdays::adjust.previous(xmon, 'cal')
      xdf <- data.frame()
      for (i in 1:length(date_seq)) {
        xdf <- rbind(xdf, download_bd(obs$BDAccountID, self$api_keys,
                                      date_seq[i]))
      }
      old_df <- try(read_holdings_file(self$bucket, dtc_name))
      if (is.null(old_df)) {
        old_df <- data.frame()
      }
      combo <- rbind(old_df, xdf)
      is_dup <- duplicated(paste0(combo$assetId, combo$returnInfo))
      combo <- combo[!is_dup, ]
      write_parquet(combo, self$bucket$path(
        paste0("holdings/", dtc_name, ".parquet")))
    },

    #' @description
    #' Pull holdings of portfolio, current sources include Black Diamon,
    #' SEC EDGAR, and Excel.
    #' @param dtc_name key id, DTCName column in master security list
    #' @param as_of optional date for when the holdings are pulled as of, see
    #'   details
    #' @param user_email default is Alejandro's, SEC requires email in API
    #' @param xl_df if updating from Excel, data.frame to pass through
    #' @param return_df data will be saved in S3, if set to TRUE a data.frame
    #'   will also be returned
    #' @details The portfolio (or mutual fund, CTF, SMA, ETF, etc) will need to
    #'   be in the MSL. For existing files, new rows will
    #'   be  added for holdings as of the new date. For SEC sources the most
    #'   recent filings will be pulled. For Black Diamond the date is part of
    #'   the API to pull holdings as of a specific date. If left NULL date will
    #'   default to the last trading day.
    update_holding = function(dtc_name, as_of = NULL, user_email = NULL,
                              xl_df = NULL, return_df = FALSE) {
      if (is.null(as_of)) {
        as_of <- last_us_trading_day()
      }
      if (is.null(user_email)) {
        user_email <- 'alejandro.sotolongo@diversifiedtrust.com'
      }
      self$check_bucket()
      ix <- self$msl$DTCName == dtc_name
      ix[is.na(ix)] <- FALSE
      obs <- self$msl[ix, ]
      if (nrow(obs) == 0) {
        stop('could not find account id in msl')
      }
      if (nrow(obs) > 1) {
        warning('found duplicate account ids in msl, taking first match')
        obs <- obs[1, ]
      }
      hist_df <- read_holdings_file(self$bucket, dtc_name)
      if (is.null(hist_df)) {
        hist_file <- FALSE
      } else {
        hist_file <- TRUE
      }
      if (obs$HoldingsSource == 'SEC') {
        df <- self$update_sec(obs, user_email)
      }
      if (obs$HoldingsSource == 'BD') {
        df <- self$update_bd(obs, self$api_keys, as_of)
        if (is.null(df)) {
          warning(paste0('no data found for ', dtc_name))
          return(NULL)
        }
      }
      # code for excel inputs
      s3_path <- paste0('holdings/', dtc_name, '.parquet')
      if (hist_file) {
        if (df$returnInfo[1] %in% hist_df$returnInfo) {
          warning(paste0(dtc_name, ' already up to date.'))
          if (return_df) {
            return(df)
          } else {
            return()
          }
        } else {
          new_df <- rob_rbind(hist_df, df)
          write_parquet(new_df, self$bucket$path(s3_path))
        }
      } else {
        warning(paste0(dtc_name, ' not found, creating new file'))
        write_parquet(df, self$bucket$path(s3_path))
      }
      if (return_df) {
        return(df)
      }
    },

    #' @description
    #' Update All CTF holdings from Black Diamond
    #' @param update_bd boolean to update underlying holdings first.
    #' @details
    #' The SMA holdings in CTF accounts are updated bi-monthly. As a
    #' work around the SMAs are read as their own accounts seperately to
    #' get a more recent estimate. If update_bd is set to FALSE the latest
    #' values will be used (last time they were updated). Set to TRUE to
    #' force and update of the underlying SMAs to the last trading day.
    update_all_ctf_holdings = function(update_bd = FALSE) {
      if (update_bd) {
        bd <- subset_df(self$msl, 'HoldingsSource', 'BD')
        for (i in 1:nrow(bd)) {
          self$update_holding(bd$DTCName[i])
        }
      }
      self$update_ctf_holdings('US Active Equity')
      self$update_ctf_holdings('US Core Equity')
    },

    #' @description Update a CTF holding
    #' @param dtc_name DTCName in msl (key id)
    #' @param add_to_existing boolean to add new holdings to old holdings as
    #' a time-series of holdings. If set to FALSE the file with old holdings
    #' will be overwritten and only new holdings will exist.
    #' @details CAREFUL with add_to_existing. Set to TRUE to continue time-series
    #' of holdings. If you need to overwrite file with just new holdings, set
    #' to FALSE.
    update_ctf_holdings = function(dtc_name, add_to_existing = TRUE) {
      obs <- subset_df(self$msl, 'DTCName', dtc_name)
      df <- download_bd(obs$BDAccountID, self$api_keys)
      mdf <- merge_msl(df, self$msl)
      bd <- subset_df(mdf$match, 'HoldingsSource', 'BD')
      for (i in 1:nrow(bd)) {
        x <- read_holdings_file(self$bucket, bd$DTCName[i], TRUE)
        mdf$all$emv[mdf$all$DTCName == bd$DTCName[i]] <- sum(x$emv)
      }
      s3_name <- paste0('holdings/', dtc_name, '.parquet')
      df <- mdf$all[, 1:9]
      if (add_to_existing) {
        old_df <- read_parquet(self$bucket$path(s3_name))
        df <- rob_rbind(old_df, df)
      }
      write_parquet(df, self$bucket$path(s3_name)
      )
    },

    #' @description Helper function to update holdings from SEC EDGAR filings
    #' @param obs row of msl with portfolio meta data
    #' @param user_email SEC requires an email address when scraping data
    #' @return data.frame of holdings, does not save to S3
    update_sec = function(obs, user_email) {
      df <- download_sec_nport(obs$LongCIK, obs$ShortCIK, user_email)
      return(df)
    },

    #' @description Helper function to update holdings from Black Diamond
    #' @param obs row of msl with portfolio meta data
    #' @param api_keys description
    #' @param as_of desired date of when to pull holdings from
    #' @return data.frame of holdings, does not save to S3
    #' @details
    #' IMPORTANT: needs to be run locally, api keys for black diamond are
    #' only stored in DTC servers, not AWS.
    update_bd = function(obs, api_keys, as_of) {
      df <- download_bd(obs$BDAccountID, api_keys, as_of)
      return(df)
    },

    #' @description Update all portfolios holdings (except CTFs, see
    #'   update_ctf_holdings) from Black Diamond and SEC
    #' @param user_email will default to Alejandro's if left NULL, SEC requires
    #' @param as_of for Black Diamond date for when to pull as of, will
    #'   default to last trading day if letf NULL
    #' @return saves updated holdings in S3
    #' @details
    #' The portfolios to be updated are filtered from the MSL that have
    #' holdings sources of BD or SEC. CTFs are updated with a seperate
    #' process to work around the bi-monthly SMA update within the CTF.
    update_all_bd_and_sec = function(user_email = NULL, as_of = NULL) {
      if (is.null(user_email)) user_email <- 'asotolongo@diversifiedtrust.com'
      ix <- self$msl$HoldingsSource == 'SEC'
      ix[is.na(ix)] <- FALSE
      sec <- self$msl[ix, ]
      if (nrow(sec) == 0) {
        warning('no SEC files found to update')
      } else {
        print('updating SEC')
        for (i in 1:nrow(sec)) {
          print(paste0('updating ', sec$DTCName[i], ' ', i, ' out of ',
                       nrow(sec)))
          self$update_holding(sec$DTCName[i], as_of, user_email)
        }
      }
      ix <- self$msl$HoldingsSource == 'BD'
      ix[is.na(ix)] <- FALSE
      bd <- self$msl[ix, ]
      if (nrow(bd) == 0) {
        warning('no BD files found to update')
      } else {
        print('updating BD')
        for (i in 1:nrow(bd)) {
          print(paste0('updating ', bd$DTCName[i], ' ', i, ' out of ',
                       nrow(bd)))
          self$update_holding(bd$DTCName[i], as_of, user_email)
        }
      }
    },

    # returns ----

    #' @description Read in all retuns to populate ret field
    #' @return db$ret will now be populated with a list containing two xts
    #'   objects: $d daily returns and $m monthly returns
    read_all_ret = function(all_stock = TRUE) {
      mf <- read_feather(self$bucket$path('returns/daily/mutual-fund.arrow'))
      mf <- dataframe_to_xts(mf)
      ctf <- read_feather(self$bucket$path('returns/daily/ctf.arrow'))
      ctf <- dataframe_to_xts(ctf)
      d <- xts_cbind(mf, ctf)
      if (all_stock) {
        sr <- read_feather(self$bucket$path('returns/daily/factset.arrow'))
        sr <- dataframe_to_xts(sr)
        d <- xts_cbind(d, sr)
      }
      if (any(duplicated(colnames(d)))) {
        warning('duplicated daily column names')
      }
      m <- read_feather(self$bucket$path('returns/monthly/ctf.arrow'))
      m <- dataframe_to_xts(m)
      hf <- read_parquet(self$bucket$path('hfr-files/hfr-all-ror.parquet'))
      hf <- pivot_wider(hf, id_cols = Date, values_from = Performance,
                        names_from = Fund)
      hf <- dataframe_to_xts(hf)
      m <- xts_cbind(m, hf)
      if (any(duplicated(colnames(m)))) {
        warning('duplicated monthly column names')
      }
      ret <- list()
      ret$d <- d
      ret$m <- m
      self$ret <- ret
    },


    #' @description Get factset request ids from the msl
    #' @param max_iter_by number to sequence large number of ids
    #' @details
    #'   Large amounts of ids need to broken up, some factset API requests only
    #'   allow 50 ids at a time. Request ids first look for ISIN, then CUSIP,
    #'   SEDOL, LEI, and Identifier in order. The function returns a list with
    #'   ids and iter for global prices and fi_ids and fi_iter for the formula
    #'   API (mutual fund returns). The iter is the sequence of large amounts
    #'   of ids to be broken up into smaller pieces. For small numbers, e.g.,
    #'   10 ids will contain and iter of c(1, 10).
    filter_fs_ids = function(max_iter_by = 50) {
      fs <- subset_df(self$msl, 'ReturnSource', 'factset')
      fmf <- subset_df(fs, 'SecType', 'Mutual Fund')
      fs <- fs[!fs$Ticker %in% fmf$Ticker, ]
      ids <- fs$ISIN
      ids[is.na(ids)] <- fs$CUSIP[is.na(ids)]
      ids[is.na(ids)] <- fs$SEDOL[is.na(ids)]
      ids[is.na(ids)] <- fs$LEI[is.na(ids)]
      ids[is.na(ids)] <- fs$Identifier[is.na(ids)]
      ids <- gsub(' ', '', ids)
      ids <- na.omit(ids)
      iter <- get_iter(ids)
      fi_ids <- fmf$Identifier
      fi_iter <- get_iter(fi_ids)
      res <- list()
      res$ids <- ids
      res$iter <- iter
      res$fi_ids <- fi_ids
      res$fi_iter <- fi_iter
      return(res)
    },

    #' @description
    #' Update mutual fund returns with Factset formula API
    #' @param days_back integer for how many trading days to pull history for,
    #'   default is zero to pull only latest trading day
    #' @details
    #'   Will add new returns to existing time-series stored in S3. New
    #'   time-series that are not in the old file will have NAs for older returns
    #'   depending on the history available and the days_back input. For
    #'   existing time-series, any overlap in the new and old returns will be
    #'   overwritten with the new returns.
    update_fs_mf_ret_daily = function(ids = NULL, days_back = 0) {
      old_ret <- read_feather(
        self$bucket$path("returns/daily/mutual-fund.arrow"))
      old_ret <- dataframe_to_xts(old_ret)
      if (is.null(ids)) {
        res <- self$filter_fs_ids()
        iter <- res$fi_iter
        ids <- res$fi_ids
      } else {
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
      }
      ret_df <- data.frame()
      formulas <- paste0('P_TOTAL_RETURNC(-', days_back, 'D,NOW,D,USD)')
      for (i in 1:(length(iter)-1)) {
        xids <- ids[iter[i]:iter[i+1]]
        json <- download_fs(
          api_keys = self$api_keys,
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
      ret_df$value <- ret_df$value / 100
      ix <- id_match_msl(ret_df$requestId, self$msl)
      ret_df$DTCName <- self$msl$DTCName[ix]
      is_dup <- duplicated(paste0(ret_df$DTCName, ret_df$date))
      ret_df <- ret_df[!is_dup, ]
      wdf <- pivot_wider(ret_df, id_cols = date, values_from = value,
                         names_from = DTCName)
      wdf <- dataframe_to_xts(wdf)
      combo <- xts_rbind(old_ret, wdf)
      combo <- xts_to_dataframe(combo)
      write_feather(combo, self$bucket$path('returns/daily/mutual-fund.arrow'))
    },

    #' @description
    #' Update returns for all investments traded on an exchange
    #' (e.g., stocks, ETFs) with Factset Global Prices API
    #' @param ids optional, can pass through string of ids, otherwise will
    #'   pull from the master security list
    #' @param date_start optional string 'YYYY-MM-DD' to represent the first
    #'   date in the time-series
    #' @param date_end optional string 'YYYY-MM-DD' to represent the last date
    #'   in the time-series.
    #' @details
    #'   The default for dates is to pull the most recent trading days return.
    #'   The routine is run overnight to add a new return to the existing
    #'   table of returns. To add new returns specify ids and set the date_start
    #'   farther back (~5 years) to create a history.
    update_fs_exchange_ret_daily = function(ids = NULL, date_start = NULL,
                                   date_end = NULL) {
      old_ret <- read_feather(self$bucket$path("returns/daily/factset.arrow"))
      old_xts <- dataframe_to_xts(old_ret)
      if (is.null(ids)) {
        res <- self$filter_fs_ids(50)
      } else {
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
      }
      if (is.null(date_start)) {
        date_start <- Sys.Date() - 1
      }
      if (is.null(date_end)) {
        date_end <- Sys.Date() - 1
      }
      df <- data.frame()
      for (i in 1:(length(res$iter)-1)) {
        json <- download_fs_gp(
          api_keys = self$api_keys,
          ids = res$ids[res$iter[i]:res$iter[i+1]],
          date_start = date_start,
          date_end = date_end,
          freq = 'D'
        )
        df <- rob_rbind(df, flatten_fs_gp(json))
        print(res$iter[i])
      }
      ix <- id_match_msl(df$requestId, self$msl)
      df$DTCName <- self$msl$DTCName[ix]
      df$totalReturn <- df$totalReturn / 100
      is_dup <- duplicated(paste0(df$DTCName, df$date))
      df <- df[!is_dup, ]
      df$date <- as.Date(df$date)
      wdf <- pivot_wider(df, id_cols = date, values_from = totalReturn,
                         names_from = DTCName)
      wxts <- dataframe_to_xts(wdf)
      combo <- xts_rbind(wxts, old_xts)
      df_out <- xts_to_dataframe(combo)
      write_feather(df_out, self$bucket$path('returns/daily/factset.arrow'))
    },


    #' @description
    #' Update monthly CTF and SMA returns
    #' @param t_minus numeric for how many months to update
    #' @param add_row TRUE = add row to existing file of returns, FALSE =
    #'   overwrite and save only new returns
    update_ctf_ret_monthly = function(t_minus = 1, add_row = TRUE) {
      ctf <- subset_df(self$msl, 'ReturnSource', 'ctf_d')
      m_id <- ctf$ISIN
      rl <- list()
      ix <- rep(TRUE, length(m_id))
      for (i in 1:length(m_id)) {
        x <- try(download_fs_ctf_ret(
          m_id[i],
          self$api_keys,
          paste0("-", t_minus),
          freq = 'M'))
        if ('try-error' %in% class(x)) {
          rl[[i]] <- NULL
          ix[i] <- FALSE
        } else {
          rl[[i]] <- x
        }
      }
      m_ret <- do.call('cbind', rl)
      colnames(m_ret) <- ctf$DTCName[ix]
      if (add_row) {
        old_ret <- read_feather(self$bucket$path('returns/monthly/ctf.arrow'))
        combo <- xts_rbind(dataframe_to_xts(old_ret), m_ret)
        df <- xts_to_dataframe(combo)

      } else {
        df <- xts_to_dataframe(m_ret)
      }
      write_feather(df, self$bucket$path('returns/monthly/ctf.arrow'))
    },

    #' @description
    #' Update CTF and SMA Returns daily
    #' @param t_minus numeric for how many days back to go
    #' @param add_row TRUE = add row to existing file of returns, FALSE =
    #'   overwrite and save only new returns
    update_ctf_ret_daily = function(t_minus = 1, add_row = TRUE) {
      ctf <- subset_df(self$msl, 'ReturnSource', 'ctf_d')
      d_id <- ctf$Identifier
      rl <- list()
      ix <- rep(TRUE, length(d_id))
      for (i in 1:length(d_id)) {
        x <- try(download_fs_ctf_ret(
          d_id[i],
          self$api_keys,
          paste0("-", t_minus),
          freq = "D"))
        if ('try-error' %in% class(x)) {
          rl[[i]] <- NULL
          ix[i] <- FALSE
        } else {
          rl[[i]] <- x
        }
      }
      d_ret <- do.call('cbind', rl)
      colnames(d_ret) <- ctf$DTCName[ix]
      if (add_row) {
        old_ret <- read_feather(self$bucket$path('returns/daily/ctf.arrow'))
        combo <- xts_rbind(dataframe_to_xts(old_ret), d_ret)
        df <- xts_to_dataframe(combo)
      } else {
        df <- xts_to_dataframe(d_ret)
      }
      write_feather(df, self$bucket$path('returns/daily/ctf.arrow'))
    },

    #' @description Pull returns from AWS into xts object
    #' @param ids tickers, cusips, isins, etc
    pull_ret = function(ids) {
      if (is.null(self$ret)) {
        self$read_all_ret()
      }
      ix <- id_match_msl(ids, self$msl)
      if (all(is.na(ix))) {
        warning('no ids found')
        return(NULL)
      }
      if (any(is.na(ix))) {
        warning(paste0(ids[is.na(ix)], ' not found'))
        ix <- na.omit(ix)
      }
      dtc_name <- self$msl$DTCName[ix]
      res <- dtc_name_match_ret(dtc_name, self$ret)
      return(res$r)
    },
    
    ## QUAL DATA ----

    #' @description
    #' Download factset sectors or country of risk
    #' @param xformula currently supports factset sector and country of risk
    #' @param ids character string of ids, if left blank will download for all
    #' stocks in MSL
    #' @return saves data.frame into s3
    #' @details ids are added to the existing list, duplicates are checked and
    #' removed
    download_factset_sector_country = function(xformula, ids = NULL) {
      if (xformula == "FG_FACTSET_SECTOR") {
        fpath <- "co-data/arrow/sector.arrow"
        fld <- "FactsetSector"
      } else if (xformula == "FREF_ENTITY_COUNTRY(RISK,NAME)") {
        fpath <- "co-data/arrow/country.arrow"
        fld <- "RiskCountry"
      } else {
        stop("bad formula")
      }
      if (is.null(ids)) {
        res <- self$filter_fs_ids(50)
      } else {
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
      }
      xdf <- data.frame()
      for (i in 1:(length(res$iter)-1)) {
        xids <- res$ids[res$iter[i]:res$iter[i+1]]
        json <- download_fs(
          self$api_keys,
          xids,
          xformula
        )
        dat <- json$data
        sec <- lapply(dat, '[[', xformula)
        sec <- unlist(list_replace_null(sec))
        req_id <- lapply(dat, '[[', 'requestId')
        req_id <- unlist(list_replace_null(req_id))
        xdf <- rbind(xdf, data.frame(requestId = req_id, x = sec))
        print(paste0(res$iter[i], ' out of ', res$iter[length(res$iter)]))
      }
      colnames(xdf) <- c("requestId", fld)
      ix <- id_match_msl(xdf$requestId, self$msl)
      xdf$DTCName <- self$msl$DTCName[ix]
      old_df <- read_feather(self$bucket$path(fpath))
      combo <- rbind(old_df, xdf)
      combo <- combo[!duplicated(combo$DTCName), ]
      write_feather(combo, self$bucket$path(fpath))
    },


    #' @description
    #' Run after each quarter to add most recent fundamental data point
    #' (e.g., P/E, ROE) to S3 table.
    #' @details
    #' The files are organized by metric. For example the P/E file will
    #' contain a time-series of P/Es for each company. This function reads all
    #' the metrics and combines the latest values for each company into one
    #' table.
    update_fs_fina_most_recent = function() {
      dat <- list()
      dat$pe <- read_feather(self$bucket$path('co-data/arrow/PE.arrow'))
      dat$pe$metric <- "PE"
      dat$pb <- read_feather(self$bucket$path('co-data/arrow/PB.arrow'))
      dat$pb$metric <- "PB"
      dat$pfcf <- read_feather(self$bucket$path('co-data/arrow/PFCF.arrow'))
      dat$pfcf$metric <- "PFCF"
      dat$dy <- read_feather(self$bucket$path('co-data/arrow/DY.arrow'))
      dat$dy$metric <- "DY"
      dat$roe <- read_feather(self$bucket$path('co-data/arrow/ROE.arrow'))
      dat$roe$metric <- "ROE"
      dat$mcap <- read_feather(self$bucket$path('co-data/arrow/MCAP.arrow'))
      dat$mcap$metric <- "MCAP"
      all_df <- do.call('rbind', dat)
      split_name <- split(all_df, all_df$DTCName)
      split_metric <- function(x) {
        split(x, x$metric)
      }
      split_met <- lapply(split_name, split_metric)
      last_date <- function(x) {
        res <- data.frame()
        for (i in 1:length(x)) {
          ix <- which(x[[i]]$date == max(x[[i]]$date, TRUE))
          res <- rob_rbind(res, x[[i]][ix, ])
        }
        return(res)
      }
      rec_list <- lapply(split_met, last_date)
      rec_df <- do.call('rbind', rec_list)
      write_feather(rec_df, self$bucket$path('co-data/arrow/latest-fina.arrow'))
    },

    #' @description
    #' Run after each quarter to add most recent fundamental data point
    #' (e.g., P/E, ROE) to S3 table.
    #' @param yrs_back integer for how many years back to pull most recent data
    #' @param dtype which metric to download, current options are PE, PB, PFCF,
    #'   DY, ROE, and MCAP
    #' @param add_row boolean to add row to existing dataset. If set to FALSE
    #'   the metric table will be over-written with new data.
    #' @details
    #' Currently set up for P/E, P/B, P/FCF, DY, ROE, and Mkt Cap. The data
    #' are organized by the metric. The P/E table will contain a time-series
    #' of the P/E for each stock.
    update_fs_fina_quarterly = function(
    ids = NULL,
    yrs_back = 1,
    dtype = c('PE', 'PB', 'PFCF', 'DY', 'ROE', 'MCAP'),
    add_row = TRUE) {
      # TO-DO read old file and and new row
      if (dtype == 'PE') {
        formulas <- paste0('FG_PE(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'PB') {
        formulas <- paste0('FG_PBK(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'PFCF') {
        formulas <- paste0('FG_CFLOW_FREE_EQ_PS(-', yrs_back, 'AY,NOW,CQ,USD)')
      } else if (dtype == 'DY') {
        formulas <- paste0('FG_DIV_YLD(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'ROE') {
        formulas <- paste0('FG_ROE(-', yrs_back, 'AY,NOW,CQ)')
      } else if (dtype == 'MCAP') {
        formulas <- paste0('FF_MKT_VAL(ANN_R,-', yrs_back, 'AY,NOW,CQ,,USD)')
      } else {
        stop("dtype must be 'PE', 'PB', 'PFCF', 'DY', 'ROE', or 'MCAP'")
      }
      if (is.null(ids)) {
        res <- self$filter_fs_ids()
        ids <- res$ids
        iter <- res$iter
      } else {
        iter <- c(1, length(ids))
      }
      val_df <- data.frame()
      for (i in 1:(length(iter)-1)) {
        xids <- ids[iter[i]:iter[i+1]]
        json <- download_fs(
          api_keys = self$api_keys,
          ids = xids,
          formulas = formulas,
          type = 'ts'
        )
        dat <- json$data
        req_id <- sapply(dat, '[[', 'requestId')
        val <- sapply(dat, '[[', formulas)
        if (is.list(val)) {
          val <- list_replace_null(val)
          val <- unlist(val)
        }
        dt <- sapply(dat, '[[', 'date')
        if (is.list(dt)) {
          dt <- list_replace_null(dt)
          dt <- unlist(dt)
        }
        i_df <- data.frame(requestId = req_id, value = val, date = dt)
        val_df <- rob_rbind(val_df, i_df)
        print(iter[i])
      }
      ix <- id_match_msl(val_df$requestId, self$msl)
      val_df$DTCName <- self$msl$DTCName[ix]
      s3_nm <- paste0('co-data/arrow/', dtype, '.arrow')
      if (add_row) {
        old_df <- read_feather(self$bucket$path(s3_nm))
        val_df <- rob_rbind(old_df, val_df)
      }
      write_feather(val_df, self$bucket$path(s3_nm))
    },

    # macro ----

    #' @description
    #' Read Macro Select Workbooks
    #' @param fpath optional string to specify file path, default is
    #'   'N:/Investment Team/DATABASES/MDB/Tables/'
    #' @param acwi optional string to specify ACWI workbook name, default is
    #'   'D_MACRO_SELECT_GLOBAL'
    #' @param r3 optional string to specify Russell indexes workbook name,
    #'   default is 'D_MACRO_SELECT_R'
    #' @param fct_name string vector to specify factor names, length must match
    #'   the number of factors
    #' @return tables are saved in S3
    update_macro = function(fpath = NULL,
                            acwi = 'D_MACRO_SELECT_GLOBAL',
                            r3 = 'D_MACRO_SELECT_R',
                            fct_name = c('A', 'B', 'C', 'D', 'E')) {
      if (is.null(fpath)) {
        fpath <- 'N:/Investment Team/DATABASES/MDB/Tables/'
      }
      acwi_df <- read_macro_wb(paste0(fpath, acwi, '.xlsx'), idx = 'MSCI ACWI',
                               fct_name)
      r3_df <- read_macro_wb(paste0(fpath, r3, '.xlsx'), idx = 'Russell 3000',
                             fct_name)
      self$check_bucket()
      self$macro <- list(r3 = r3_df, acwi = acwi_df)
      write_parquet(r3_df, self$bucket$path('macro_r3.parquet'))
      write_parquet(acwi_df, self$bucket$path('macro_acwi.parquet'))
    }

    # TO DO reports ----
    # stop downloading returns for tickers in MSL that arent' held or returning NULL values
    # create seperate archived MSL?
    # check for new tickers / securities found in holdings that we don't have returns for
  )
)
