try_merge <- function(x, y, by, all = TRUE) {
  combo <- try(merge(x, y, by, all), silent = 'TRUE')
  return(combo)
}

#' @export
subset_df <- function(df, col, target) {
  ix <- df[, col] == target
  ix[is.na(ix)] <- FALSE
  return(df[ix, ])
}


#' @export
extract_list <- function(x, nm) {
  y <- lapply(x, '[[', nm)
  y[sapply(y, is.null)] <- NA
  unlist(y)
}

#' @export
list_replace_null <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

#' @export
month_end <- function(dt) {
  lubridate::ceiling_date(as.Date(dt), 'months') - 1
}


#' @export
get_iter <- function(ids, max_iter_by = 50) {
  if (length(ids) > max_iter_by) {
    iter <- iter <- seq(1, length(ids), (max_iter_by-1))
    if (iter[length(iter)] < length(ids)) {
      iter <- c(iter, length(ids))
    }
    ret_list <- list()
  } else {
    iter <- c(1, length(ids))
  }
  return(iter)
}


#' @export
check_holdings_df <- function(hdf) {
  ix <- c('pctVal', 'returnInfo') %in% colnames(df)
  if (!all(ix)) {
    stop('data.frame missing pctVal and / or returnInfo')
  }
  ix <- c('ticker', 'isin', 'cusip', 'name', 'lei', 'identifier') %in%
    colnames(df)
  if (!any(ix)) {
    stop('need ticker, isin, cusip, name, lei, or identifier field')
  }
  return('pass')
}
