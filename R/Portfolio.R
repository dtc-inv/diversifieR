#' Portfolio Object
#' @description
#' Investment portfolio
#'
#' @export
Portfolio <- R6::R6Class(
  'Portfolio',
  public = list(
    #' @field name name of portfolio for display
    name = 'port',
    #' @field asset_ret xts of asset returns
    asset_ret = xts(),
    #' @field asset_wgt xts of weights for corresponding asset returns
    asset_wgt = xts(),
    #' @field port_ret xts of portfolio returns as a result of rebalancing
    port_ret = xts(),
    #' @field port_wealth xts of cumulative portfolio returns
    port_wealth = xts(),
    #' @field perf_stats list with performance reports
    perf_stats = list(),
    #' @field reb_stats list with rebalance stats for attribution
    reb_stats = list(),
    #' @field risk_stats list multi-variate risk stats
    risk_stats = list(),
    #' @field opt list with optimization output
    opt = list(),
    #' @field ret_freq return frequency
    ret_freq = NULL,
    #' @field reb_freq optional field for automatic rebalancing frequency
    reb_freq = NULL,
    #' @field reb_wgt xts of rebalance weights
    reb_wgt = NULL,
    #' @field rf xts of risk-free time-series
    rf = xts(),
    #' @field benchmark optional field for benchmark (also a Portfolio Object)
    benchmark = NULL,

    initialize = function(
      name = 'port',
      asset_ret = NULL,
      ret_freq = c('D', 'M', 'Q', 'A'),
      reb_freq = c(NULL, 'D', 'M', 'Q', 'A'),
      reb_wgt = NULL,
      rf = NULL,
      benchmark = NULL)
    {
      if (is.null(asset_ret)) asset_ret <- xts()
      if (is.null(rf)) rf <- xts()
      self$name <- name
      self$asset_ret <- asset_ret
      self$ret_freq <- ret_freq[1]
      self$reb_freq <- reb_freq[1]
      self$reb_wgt <- reb_wgt
      self$rf <- rf
      self$benchmark <- benchmark
    },

    clean_asset_ret = function(asset_ret = NULL, eps = 0.05) {
      if (is.null(asset_ret)) {
        asset_ret <- self$asset_ret
      }
      if (is.null(asset_ret) | nrow(asset_ret) == 0) {
        stop('no asset returns to clean')
      }
      res <- rm_na_col(asset_ret, eps)
      if (ncol(res$miss_ret) > 0) {
        warning('removed returns ', paste0(colnames(res$miss_ret), ', '))
      }
      self$asset_ret <- res$ret
      invisible(self)
    },

    #' @description
        #' Align rebalance weights with corresponding asset returns
    #' @field sum_to_1 boolean to re-calculate weights to pro-rata sum to 1 if
    #'   any returns or weights are missing
    #' @details
    #'   Weights are either a named vector or xts time-series. The alignment
    #'   will look for the intersection of names. Any names not found in the
    #'   intersection will be removed. If sum_to_1 is set to TRUE the weights will
    #'   be recalculated to sum to 100%. If the vector is not named a
    #'   warning will be issued and the length of the weights and number of
    #'   columns of the returns will be checked.
    align_reb_wgt = function(sum_to_1 = TRUE) {
      if (is.null(self$asset_ret) || is_null_dim(self$asset_ret)) {
        warning('no asset_returns found')
        return()
      }
      if ('xts' %in% class(self$reb_wgt)) {
        miss <- setdiff(colnames(self$reb_wgt), colnames(self$asset_ret))
        if (identical(miss, colnames(self$reb_wgt))) {
          stop('no intersection of $reb_wgt and $asset_ret columns')
        }

        inter <- intersect(colnames(self$reb_wgt), colnames(self$asset_ret))
        unn <- union(colnames(self$reb_wgt), colnames(self$asset_ret))
        if (length(unn) > length(inter)) {
          warning(c(unn[!unn %in% inter], ' not in intersection'))
        }
        self$reb_wgt <- self$reb_wgt[, inter]
        self$asset_ret <- self$asset_ret[, inter]
        if (sum_to_1) {
          self$reb_wgt <- self$reb_wgt / rowSums(self$reb_wgt, na.rm = TRUE)
        }
      } else if (is.vector(self$reb_wgt)) {
        if (is.null(names(self$reb_wgt))) {
          warning('reb_wgt is missing names, assuming they are lined up
                   with asset_ret')
          if (length(self$reb_wgt) != ncol(self$asset_ret)) {
            stop('length of $reb_wgt and $asset_ret columns do not match')
          }
        } else {
          inter <- intersect(names(self$reb_wgt), colnames(self$asset_ret))
          if (length(inter) == 0) {
            print(names(self$reb_wgt))
            print(names(self$asset_ret))
            print('no intersection between asset_ret and reb_wgt')
            return(NULL)
          }
          unn <- union(names(self$reb_wgt), colnames(self$asset_ret))
          if (length(unn) > length(inter)) {
            warning(c(unn[!unn %in% inter], ' not in intersection'))
          }
          self$reb_wgt <- self$reb_wgt[inter]
          self$asset_ret <- self$asset_ret[, inter]
          if (sum_to_1) {
            self$reb_wgt <- self$reb_wgt / sum(self$reb_wgt, na.rm = TRUE)
          }
        }
      }
      invisible(self)
    },

    check_wgt_ret = function() {
      if (is.null(self$asset_ret) || is_null_dim(self$asset_ret)) {
        stop('no asset_returns found')
      }
      if (is.null(self$reb_wgt) | length(self$reb_wgt) == 0) {
        stop('no rebalance weights found')
      }
      if (!all(colnames(self$asset_ret) == colnames(self$reb_wgt))) {
        stop('rebalance weights and returns do not match, try align_reb_wgt()')
      }
    },

    lazy_reb_wgt = function(reb_wgt = NULL, reb_freq = NULL) {

      # lazy_reb_wgt helps to set up a rebalance weight xts
      # reb_wgt = optional parameter to pass through a vector or xts of
      #   rebalance weights, if left to default of NULL, then the function will
      #   use self$rebal_wgt
      # reb_freq = optional parameter to set a constant rebalance frequency, if
      #  a frequency is specified a rebalance on each month, quarter, year, etc,
      #  will be added using weights from the prior rebalance before that period

      if (is.null(self$asset_ret) || is_null_dim(self$asset_ret)) {
        warning('no asset returns found')
        return(NULL)
      }

      # NULL rebalance handle as buy and hold, or don't add additional
      # dates to rebal
      if (is.null(reb_freq)) {
        reb_freq <- self$reb_freq
        if (is.null(reb_freq)) {
          reb_freq <- 'BH'
        }
      }
      reb_freq <- check_reb_freq(reb_freq)

      if (is.null(reb_wgt)) {
        reb_wgt <- self$reb_wgt
      }

      # make sure weight and return columns match
      self$align_reb_wgt()

      # set start and end dates based on returns
      date_start <- zoo::index(self$asset_ret)[1]
      date_end <- zoo::index(self$asset_ret)[nrow(self$asset_ret)]
      # if rebalance weights are a vector, convert into one row xts
      if (is.vector(reb_wgt)) {
        reb_wgt <- xts(matrix(reb_wgt, nrow = 1), date_start)
        colnames(reb_wgt) <- colnames(self$asset_ret)
        self$reb_wgt <- reb_wgt
      }

      # handle rebalance frequency, create date vector to match freq
      if (reb_freq == 'D') {
        dt_vec <- zoo::index(self$asset_ret)
      } else if (reb_freq == 'M') {
        dt_vec <- seq(date_start, date_end, 'months')
        dt_vec <- lubridate::floor_date(dt_vec + 10, 'months') - 1
      } else if (reb_freq == 'Q') {
        dt_vec <- seq(date_start, date_end, 'quarters')
        dt_vec <- lubridate::floor_date(dt_vec + 10, 'quarters') - 1
      } else if (reb_freq == 'A') {
        dt_vec <- seq(date_start, date_end, 'years')
      }
      # if buy and hold, no action needed, return weights
      if (reb_freq == 'BH') {
        self$reb_wgt <- reb_wgt
        return(invisible(self))
      }
      # else if periodic rebalances are needed
      # if vector of weights are passed, set up constant rebalance matrix
      if (nrow(reb_wgt) == 1 || is.vector(reb_wgt)) {
        reb_mat <- matrix(reb_wgt, nrow = length(dt_vec),
                          ncol = ncol(self$asset_ret), byrow = TRUE)
        reb_wgt <- xts(reb_mat, dt_vec)
        colnames(reb_wgt) <- colnames(self$asset_ret)
      # else if multiple rebalances are already in the matrix, need to add
      # periodic rebalances to the existing matrix
      } else if (nrow(reb_wgt) > 1) {
        add_reb <- xts(matrix(as.numeric(NA), nrow = length(dt_vec),
                              ncol = ncol(self$asset_ret)),
                       dt_vec)
        # remove any potential duplicate dates if existing rebalance on
        # period end
        is_dup <- zoo::index(add_reb) %in% zoo::index(reb_wgt)
        reb_combo <- rbind(add_reb[!is_dup, ], reb_wgt)
        if (all(is.na(reb_combo[1, ]))) {
          reb_combo[1, ] <- reb_wgt[1, ]
        }
        reb_xts <- fill_na_price(reb_combo)
        colnames(reb_xts) <- colnames(reb_wgt)
        reb_wgt <- reb_xts
      # final else for error handling if input is not a vector or xts
      } else {
        warning('rebalance weights are not a vector or xts,
                did not create weights')
        return()
      }
      self$reb_wgt <- reb_wgt
      invisible(self)
    },

    rebal = function(asset_ret = NULL, reb_wgt = NULL, align_wgt = TRUE,
                     sum_to_1 = TRUE, clean_asset_ret = TRUE, eps = 0.05) {

      if (!is.null(asset_ret)) {
        self$asset_ret <- asset_ret
      }
      if (!is.null(reb_wgt)) {
        self$reb_wgt <- reb_wgt
      }
      asset_ret <- self$asset_ret
      reb_wgt <- self$reb_wgt

      if (clean_asset_ret) {
        self <- self$clean_asset_ret(asset_ret, eps)
        asset_ret <- self$asset_ret
      }

      if (align_wgt) {
        self$align_reb_wgt(sum_to_1)
        reb_wgt <- self$reb_wgt
      }
      self$check_wgt_ret()

      reb_dt <- zoo::index(reb_wgt)
      ret_dt <- zoo::index(asset_ret)
      n_obs <- nrow(asset_ret)
      reb_counter <- 1
      # asset index is the end of period wealth value
      asset_idx <- matrix(nrow = n_obs + 1, ncol = ncol(asset_ret))
      asset_idx[1, ] <- 100 * reb_wgt[1, ]
      # asset weight is beginning of period weight
      asset_wgt <- matrix(nrow = n_obs, ncol = ncol(asset_ret))
      colnames(asset_wgt) <- colnames(asset_ret)
      # assumes rebalance happens at beginning of each day
      # (or month, year, etc),
      # and then that day's return is applied to create an end of day
      # wealth value
      for (i in 1:n_obs) {
        asset_idx[i + 1, ] <- asset_idx[i, ] * (1 + asset_ret[i, ])
        asset_wgt[i, ] <- asset_idx[i, ] / sum(asset_idx[i, ])
        if (reb_counter <= length(reb_dt) & reb_dt[reb_counter] <= ret_dt[i]) {
          asset_idx[i + 1, ] <- as.numeric(sum(asset_idx[i, ])) *
            as.numeric(reb_wgt[reb_counter, ]) * as.numeric((1 + asset_ret[i, ]))
          asset_wgt[i, ] <- reb_wgt[reb_counter, ]
          reb_counter <- reb_counter + 1
        }
      }
      asset_wgt <- xts(asset_wgt, zoo::index(asset_ret))
      dt_rng <- c(ret_dt[1] - 1, ret_dt)
      port_wealth <- xts(rowSums(asset_idx), dt_rng)
      colnames(port_wealth) <- self$name
      # contr to ret = portfolio wealth at end of last period *
      # weight at beginning of current period * current period return
      # port_wealth is lagged, so i = last period
      ctr_mat <- matrix(nrow = n_obs, ncol = ncol(asset_ret))
      for (i in 1:n_obs) {
        ctr_mat[i, ] <- as.numeric(port_wealth[i]) * asset_wgt[i, ] *
          asset_ret[i, ]
      }
      colnames(ctr_mat) <- colnames(asset_ret)
      self$reb_stats$ctr_mat <- ctr_mat
      colnames(asset_idx) <- colnames(asset_ret)
      asset_idx <- xts(asset_idx, c(ret_dt[1]-1, ret_dt))
      self$reb_stats$asset_idx <- asset_idx
      self$asset_ret <- asset_ret
      self$reb_wgt <- reb_wgt
      self$port_wealth <- port_wealth
      self$port_ret <- price_to_ret(port_wealth)
      self$asset_wgt <- asset_wgt
      invisible(self)
    }
  )
)



