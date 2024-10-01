#' @title Match ids to MSL
#' @param ids ids to match
#' @param msl Master Security List from Database
#' @details
#' ids are searched in the following order: DTCName, Ticker, ISIN, CUSIP, SEDOL, 
#' LEI, and Identifier
#' @export
id_match_msl <- function(ids, msl) {
  incomps <- c(NA, "000000000", "N/A", "0")
  ix_dtc <- match(ids, msl$DTCName, incomparables = incomps)
  ix_ticker <- match(ids, msl$Ticker, incomparables = incomps)
  ix_isin <- match(ids, msl$ISIN, incomparables = incomps)
  ix_cusip <- match(ids, msl$CUSIP, incomparables = incomps)
  ix_sedol <- match(ids, msl$SEDOL, incomparables = incomps)
  ix_lei <- match(ids, msl$LEI, incomparables = incomps)
  ix_id <- match(ids, msl$Identifier, incomparables = incomps)
  ix <- rep(NA, length(ids))
  ix <- .fill_ix(ix, ix_dtc)
  ix <- .fill_ix(ix, ix_ticker)
  ix <- .fill_ix(ix, ix_isin)
  ix <- .fill_ix(ix, ix_cusip)
  ix <- .fill_ix(ix, ix_sedol)
  ix <- .fill_ix(ix, ix_lei)
  ix <- .fill_ix(ix, ix_id)
  return(ix)
}


#' @title Read Piper Sandler Macro Select Workbook
#' @param wb workbook full file name including path
#' @param idx string representing which index to use, e.g., "Russell 3000"
#' @param fact_nm string vector representing names of the current macro factors
#' @export
read_macro_wb <- function(wb, idx, fact_nm) {
  menu <- readxl::read_excel(wb, 'menu')
  menu <- as.data.frame(menu)
  col_off <- menu[menu[, 2] == idx, 3]
  col_off <- na.omit(col_off)
  dat <- readxl::read_excel(wb, 'data', skip = 4)
  model <- dat[, c(1:7, (col_off-1):(col_off+3))]
  model <- as.data.frame(model)
  colnames(model)[8:12] <- fact_nm
  return(model)
}


#' @title Match securities in a portfolio with the Master Security List
#' @param df data.frame of securities, see details
#' @param msl Master Security List
#' @details
#' The match will attempt to match the following `df` columns
#'   ticker, cusip, name, isin, lei, and identifier
#' @return numeric vector with row numbers of the match in the `msl`
#' @export
msl_match <- function(df, msl) {
  incomps <- c(NA, "000000000", "N/A", "0")
  ix_ticker <- match(df$ticker, msl$Ticker, incomparables = incomps)
  ix_cusip <- match(df$cusip, msl$CUSIP, incomparables = incomps)
  ix_name <- match(toupper(df$name), toupper(msl$DTCName), incomparables = incomps)
  ix_isin <- match(df$isin, msl$ISIN, incomparables = incomps)
  ix_lei <- match(df$lei, msl$LEI, incomparables = incomps)
  ix_idnt <- match(df$identifier, msl$Identifier, incomparables = incomps)
  ix_idnt2 <- match(df$identifier, msl$SEDOL, incomparables = incomps)
  ix_idnt3 <- match(df$identifier, msl$LEI, incomparables = incomps)
  ix_idnt4 <- match(df$identifier, msl$CUSIP, incomparables = incomps)
  ix_asset_id <- match(df$assetId, msl$BDAssetID, incomparables = incomps)
  ix <- rep(NA, nrow(df))
  ix <- .fill_ix(ix, ix_ticker)
  ix <- .fill_ix(ix, ix_cusip)
  ix <- .fill_ix(ix, ix_name)
  ix <- .fill_ix(ix, ix_isin)
  ix <- .fill_ix(ix, ix_lei)
  ix <- .fill_ix(ix, ix_idnt)
  ix <- .fill_ix(ix, ix_idnt2)
  ix <- .fill_ix(ix, ix_idnt3)
  ix <- .fill_ix(ix, ix_idnt4)
  ix <- .fill_ix(ix, ix_asset_id)
  return(ix)
}


#' @title Helper function to filter ids by multiple attempts to match
#' @export
.fill_ix <- function(a, b) {
  if (length(a) == length(b)) {
    a[is.na(a)] <- b[is.na(a)]
    return(a)
  } else {
    return(a)
  }
}


#' @title Merge MSL and holdings data.frame (df)
#' @param df holdings data.frame
#' @param msl master security list
#' @return list with all, match, and miss to represent union, intersection, and
#'   union - intersection
#' @export
merge_msl <- function(df, msl) {
  ix <- msl_match(df, msl)
  union_df <- cbind(df, msl[ix, ])
  inter_df <- union_df[!is.na(ix), ]
  miss_df <- union_df[is.na(ix), ]
  res <- list(
    all = union_df,
    match = inter_df,
    miss = miss_df
  )
  return(res)
}


#' @title Left join master security list
#' @param mdf list returned from `merge_msl`, see details
#' @param msl master security list
#' @details This function is designed to rejoin data to the MSL where some of
#'   the data has been joined and some has not, for example in when drilling
#'   down in holdings the holdings will not have been joined but higher level
#'   securities will have the joined data
#' @return list with all, matched, and missing data
#' @export
left_merge_msl <- function(mdf, msl) {
  ix <- msl_match(mdf$all, msl)
  left_mdf <- mdf$all[, !colnames(mdf$all) %in% colnames(msl)]
  union_df <- cbind(left_mdf, msl[ix, ])
  inter_df <- union_df[!is.na(ix), ]
  miss_df <- union_df[is.na(ix), ]
  res <- list(
    all = union_df,
    match = inter_df,
    miss = miss_df
  )
  return(res)
}


#' @title Drilldown one layer
#' @param mdf list from `merge_msl`
#' @param bucket s3 bucket from Database object
#' @param latest boolean for use latest assets only
#' @param lay which layer to drill down
#' @param recons_wgt boolean to force weights to sum to 1
#' @export
drill_down <- function(mdf, msl, bucket, latest = TRUE, lay = 2, 
                       recons_wgt = TRUE) {
  # if we only want the latest holdings, trim to last date
  if (latest) {
    mdf$all <- latest_holdings(mdf$all)
    mdf$match <- latest_holdings(mdf$match)
    mdf$miss <- latest_holdings(mdf$miss)
  }
  # seperate funds that need drill down and extract holdings
  lay_x <- mdf$match$Layer >= lay
  if (!any(lay_x)) {
    warning('no layers found to drill down, returning mdf with added Parent 
            field')
    mdf$match$Parent <- "Portfolio"
    return(mdf)
  }
  dat <- lapply(mdf$match$DTCName[lay_x], read_holdings_file, 
                bucket = db$bucket, latest = latest)
  dat <- lapply(dat, clean_emv)

  # adjust market value and weights for each drill down fund
  # also add parent company name to drill down holdings
  val_vec <- mdf$match$emv[lay_x]
  wgt_vec <- mdf$match$pctVal[lay_x]
  par_vec <- mdf$match$DTCName[lay_x]
  for (i in 1:length(dat)) {
    if (recons_wgt) {
      dat[[i]]$pctVal <- dat[[i]]$pctVal / sum(dat[[i]]$pctVal)
    }
    dat[[i]]$emv <- dat[[i]]$pctVal * val_vec[i]
    dat[[i]]$pctVal <- dat[[i]]$pctVal * wgt_vec[i]
    dat[[i]]$Parent <- par_vec[i]
  }
  # to-do why doesn't do.call work here?
  dd_holdings <- data.frame()
  for (i in 1:length(dat)) {
    dd_holdings <- rob_rbind(dd_holdings, dat[[i]])
  }
  # sub drill down holdings back in for the one high level line-item
  mdf$match$returnInfo <- as.Date(mdf$match$returnInfo)
  dd_holdings$returnInfo <- as.Date(dd_holdings$returnInfo)
  mdf$match <- rob_rbind(mdf$match[!lay_x, ], dd_holdings)
  mdf$match$Parent[is.na(mdf$match$Parent)] <- 
    mdf$match$DTCName[is.na(mdf$match$Parent)]
  mdf$all$returnInfo <- as.Date(mdf$all$returnInfo)
  mdf$all <- rob_rbind(mdf$match, mdf$miss)
  mdf <- left_merge_msl(mdf, msl)
  return(mdf)
}


#' @title Standardize EMV column header for $ value
#' @export
#' @details holdings files from SEC will have `valUSD` as column header or field
#'   representing value, change to `emv` to standardize with Black Diamond pull
clean_emv <- function(df) {
  if ('emv' %in% colnames(df)) {
    return(df)
  }
  if ('valUSD' %in% colnames(df)) {
    df$emv <- as.numeric(df$valUSD)
  }
  return(df)
}


#' @title Merge financial data to holdings data frame
#' @param mdf list output from merge_msl containing holdings data that has
#'   been merged with the msl
#' @param bucket S3 file system from Database Object or arrow
#' @return mdf list with merged financial data in the mdf$match data.frame
#' @export
merge_fina_rec <- function(mdf, bucket) {
  fina <- read_feather(bucket$path('co-data/arrow/latest-fina.arrow'))
  is_dup <- duplicated(paste0(fina$date, fina$metric, fina$DTCName))
  fina_w <- pivot_wider(fina[!is_dup, ], id_cols = DTCName, values_from = value,
                        names_from = metric)
  mdf$match <- dplyr::left_join(mdf$match, fina_w, by = 'DTCName')
  return(mdf)
}


#' @title Merge sector data
#' @param ddf list output from drill_down or merge_msl for a group of stocks
#' @param bucket S3 file system from Database Object or arrow
#' @return ddf list with merged sectors in ddf$match data.frame
#' @export
merge_sector <- function(ddf, bucket) {
  sec <- read_feather(bucket$path('co-data/arrow/sector.arrow'))
  ddf$match <- dplyr::left_join(ddf$match, sec[, c('FactsetSector', 'DTCName')],
                                by = 'DTCName')
  return(ddf)
}


#' @title Merge country data
#' @param ddf list output from drill_down or merge_msl for a group of stocks
#' @param bucket S3 file system from Database Object or arrow
#' @return ddf list with merged sectors in ddf$match data.frame
#' @export
merge_country <- function(ddf, bucket) {
  country <- read_feather(bucket$path("co-data/arrow/country.arrow"))
  ddf$match <- dplyr::left_join(ddf$match, country[, c('RiskCountry', 'DTCName')],
                                by = 'DTCName')
  return(ddf)
}


#' @export
read_holdings_file <- function(bucket, dtc_name, latest = FALSE) {
  s3_path <- paste0("holdings/", dtc_name, ".parquet")
  s3_exists <- s3_path %in% bucket$ls("holdings/")
  if (s3_exists) {
    df <- read_parquet(bucket$path(s3_path))
    if (latest) {
      df <- latest_holdings(df)
    }
    return(df)
  } else {
    warning(paste0(s3_path), " not found.")
    return()
  }
}


#' @export
rob_rbind <- function(df1, df2) {
  if (nrow(df1) == 0) {
    return(df2)
  }
  if (nrow(df2) == 0) {
    return(df1)
  }
  nm_union <- unique(c(colnames(df1), colnames(df2)))
  df1_miss <- !nm_union %in% colnames(df1)
  df2_miss <- !nm_union %in% colnames(df2)
  df1[, nm_union[df1_miss]] <- NA
  df2[, nm_union[df2_miss]] <- NA
  df2 <- df2[, colnames(df1)]
  rbind(df1, df2)
}


#' @export
check_mdf <- function(mdf) {
  if (!is.list(mdf)) {
    stop('mdf is not a list')
  }
  if (!all(c("all", "match", "miss") %in% names(mdf))) {
    stop("mdf slots not properly set up")
  }
  return("mdf pass")
}


#' @export
latest_holdings <- function(df) {
  if (any(is.na(df$returnInfo))) {
    warning('some returnInfo missing')
  }
  if ('ParrentAsset' %in% colnames(df)) {
    pa <- na.omit(unique(df$ParrentAsset))
    for (i in 1:length(pa)) {
      is_pa <- df$ParrentAsset == pa[i]
      is_pa[is.na(is_pa)] <- FALSE
      df_pa <- df[is_pa, ]
      is_old <- df_pa[, 'returnInfo'] < max(df_pa[, 'returnInfo'], na.rm = TRUE)
      df_pa <- df_pa[!is_old, ]
      if (i == 1) {
        res <- df_pa
      } else {
        res <- rbind(res, df_pa)
      }
    }
  } else {
    ix <- df$returnInfo == max(df$returnInfo, na.rm = TRUE)
    if (any(is.na(ix))) {
      warning('some returnInfo missing')
      ix[is.na(ix)] <- FALSE
    }
    res <- df[ix, ]
  }
  return(res)
}


#' @title Group holdings data.frame
#' @param ddf holdings data.frame that has been drilled down, see 
#'   drill_down
#' @param bdf benchmark data.frame
#' @param gp_by string to represent which column to group data by
#' @param recons_wgt boolean for weights to sum to 1
#' @export
group_df <- function(ddf, gp_by, bucket, bdf = NULL, recons_wgt = TRUE) {
  x <- merge_fina_rec(ddf, bucket)
  x <- merge_sector(x, bucket)
  x <- merge_country(x, bucket)
  x <- x$match
  x_parent <- split(x, x$Parent)
  x_par_wgt <- sapply(x_parent, function(x) {sum(x$pctVal, na.rm = TRUE)})
  if (gp_by == "Ticker") {
    xdf <- pivot_wider(x, id_cols = DTCName, names_from = Parent, 
                       values_from = pctVal, values_fn = sum)
    xdf$Total <- rowSums(xdf[, -1], na.rm = TRUE)
    if (recons_wgt) {
      tot <- colSums(xdf[, 2:(ncol(xdf)-1)], na.rm = TRUE)
      xdf[, 2:(ncol(xdf)-1)] <- xdf[, 2:(ncol(xdf)-1)] / 
        matrix(tot, nrow = nrow(xdf), ncol = length(tot), byrow = TRUE)
    }
    ix <- match(xdf$DTCName, x$DTCName)
    xdf$Ticker = x$Ticker[ix]
    xdf$PE <- x$PE[ix]
    xdf$PB <- x$PB[ix]
    xdf$ROE <- x$ROE[ix]
    xdf$MktCap <- x$MCAP[ix]
    xdf$Country <- x$RiskCountry[ix]
    xdf <- relocate(xdf, DTCName, Ticker, Total)
  } else if (gp_by == "Sector") {
    xdf <- pivot_wider(x, id_cols = FactsetSector, names_from = Parent, 
                       values_from = pctVal, values_fn = sum)
    xdf$Total <- rowSums(xdf[, -1], na.rm = TRUE)
    if (recons_wgt) {
      tot <- colSums(xdf[, 2:(ncol(xdf)-1)], na.rm = TRUE)
      xdf[, 2:(ncol(xdf)-1)] <- xdf[, 2:(ncol(xdf)-1)] / 
        matrix(tot, nrow = nrow(xdf), ncol = length(tot), byrow = TRUE)
    }
    xdf <- relocate(xdf, FactsetSector, Total)
    return(xdf)
  } else if(gp_by == "Country") {
    xdf <- pivot_wider(x, id_cols = RiskCountry, names_from = Parent, 
                       values_from = pctVal, values_fn = sum)
    xdf$Total <- rowSums(xdf[, -1], na.rm = TRUE)
    if (recons_wgt) {
      tot <- colSums(xdf[, 2:(ncol(xdf)-1)], na.rm = TRUE)
      xdf[, 2:(ncol(xdf)-1)] <- xdf[, 2:(ncol(xdf)-1)] / 
        matrix(tot, nrow = nrow(xdf), ncol = length(tot), byrow = TRUE)
    }
    xdf <- relocate(xdf, RiskCountry, Total)
  } else if (gp_by == "Parent") {
    pct_val <- sapply(x_parent, \(x) sum(x$pctVal))
    
  } else {
    return(NULL)
  }
  return(xdf)
}

wgt_avg <- function(w, x) {
  x <- x[!is.na(x)]
  w <- w[!is.na(w)]
  w <- w / sum(w)
  sum(x * w)
}

#' @title Split data.frame into n groups from a numeric field
#' @param x data.frame
#' @param tgt target column name to form split on
#' @param n integer number of groups to form
#' @return list of split data.frames
#' @details Groups are sorted in ascending order by quantile function. Missing
#' values are removed.
#' @export
split_by_n <- function(x, tgt, n) {
  x <- x[!is.na(x[, tgt]), ]
  p <- seq(0, 1, 1/(n-1))
  split(x, cut(x[, tgt], quantile(x[, tgt], probs = p)))
}


#' @title Average P/E
#' @param w weight vector
#' @param x P/E vector
#' @return weighted average P/E
#' @details
#' Filters out missing and negative values and re-weights
avg_pe <- function(w, x) {
  is_miss <- is.na(x) | x <= 0
  x <- x[!is_miss]
  w <- w[!is_miss]
  w <- w / sum(w, na.rm = TRUE)
  wgt_har_mean(w, x)
}


#' @title Weighted Harmonic Mean
#' @param w weight
#' @param x data to weight
#' @export
wgt_har_mean <- function(w, x) {
  sum(w, na.rm = TRUE) / sum(w / x, na.rm = TRUE)
}


#' @title Merge list with data.frames containing 2 columns
#' @param x list of data.frames
#' @details assumes key ids are in column 1 and the data you want to merge
#' is in column 2
#' @return data.frame with merged data
#' @export
merge_list <- function(x) {
  id <- unique(unlist(lapply(x, function(x){x[[1]]})))
  res <- matrix(nrow = length(id), ncol = length(x))
  for (i in 1:length(x)) {
    ix <- match(id, x[[i]][[1]])
    res[na.omit(ix), i] <- x[[i]][[2]]
  }
  res <- data.frame(id = id, res)
  if (length(names(x)) == length(x)) {
    colnames(res)[-1] <- names(x)   
  }
  return(res)
}
