#' @title Calculate Volatility or TE Weights
#' @param x vector of weights corresponding to `cov_mat`
#' @param cov_mat covariance matrix
#' @return a vector of risk weights
#' @details
#' For TE `x` is a vector of active weights (weight - benchmark weight) and
#' the `cov_mat` is a union of the portfolio and benchmark positions that
#' corresponds to the active weight vector `x`
#' @export
risk_wgt <- function(x, cov_mat) {
  wgt <- matrix(x, ncol = 1)
  (wgt * (cov_mat %*% wgt)) / (t(wgt) %*% cov_mat %*% wgt)[1]
}


#' @title Calculate Factor Risk Weight
#' @param x_beta factor betas from regression
#' @param x_cov covariance matrix of the regression factors
#' @param y_variance variance of the y variable
#' @return a vector of the factor risk weights
#' @details
#' Risk weights are the contribution to the total variance of the y variable,
#' i.e., the fund or asset to explain
#' @export
factor_risk_wgt <- function(x_beta, x_cov, y_variance) {
  xbeta <- matrix(xbeta, nrow = length(x_beta), ncol = 1)
  (x_beta * (x_cov %*% x_beta)) / y_variance[1]
}


#' @title Portfolio Volatility
#' @param x vector of weights
#' @param cov_mat covariance matrix
#' @export
port_risk <- function(x, cov_mat) {
  wgt <- matrix(x, ncol = 1)
  sqrt((t(wgt) %*% cov_mat %*% wgt)[1])
}


#' @title Portfolio Return
#' @param x vector of weights
#' @param mu_vec vector of expected or historic means
#' @export
port_ret <- function(x, mu_vec) {
  wgt <- matrix(x, ncol = 1)
  mu <- matrix(mu_vec, ncol = 1)
  t(wgt) %*% mu
}

#' @title Correlation between two portfolios
#' @param wgt_vec_1 weight vector of 1st portfolio
#' @param wgt_vec_2 weight vector of 2nd portfolio
#' @param cov_mat union covariance of wgt_vec_1 and wgt_vec_2
#' @export
port_corr <- function(wgt_vec_1, wgt_vec_2, cov_mat) {
  w1 <- as.matrix(wgt_vec_1, ncol = 1)
  w2 <- as.matrix(wgt_vec_2, ncol = 1)
  (t(w1) %*% cov_mat %*% w2) /
    (sqrt(t(w1) %*% cov_mat %*% w1) * sqrt(t(w2) %*% cov_mat %*% w2))
}


#' @title Beta between two portfolios
#' @param wgt_vec_1 weight vector of 1st portfolio
#' @param wgt_vec_2 weight vector of 2nd portfolio
#' @param cov_mat union covariance of wgt_vec_1 and wgt_vec_2
#' @export
port_beta <- function(wgt_vec_1, wgt_vec_2, cov_mat) {
  w1 <- as.matrix(wgt_vec_1, ncol = 1)
  w2 <- as.matrix(wgt_vec_2, ncol = 1)
  (t(w2) %*% cov_mat %*% w1) / (t(w2) %*% cov_mat %*% w2)
}


#' @title Cluster around latents
#' @param cor_mat correlation matrix
#' @export
pca_hclust <- function(cor_mat) {
  p <- princomp(covmat = cor_mat)
  meas <- diag(p$sdev) %*% t(p$loadings[,])
  dist_res <- dist(t(meas), method = 'euclidean')
  hclust(dist_res)
}


#' @title Calculate risk cluster weights
#' @param hc tree from hclust output
#' @param vol volatility of assets used in hc calculation
#' @param k number of clusters
#' @export
risk_cluster_wgt <- function(hc, vol, k = 2) {

  n_assets <- max(hc$order)
  memb <- cutree(hc, k)
  xcor <- diag(1, n_assets, n_assets)
  for (i in 1:k) {
    xcor[memb == i, memb == i] <- 1
  }
  vol <- matrix(vol, ncol = 1)
  xcov <- vol %*% t(vol) * xcor
  mu_vec <- vol * 0.25
  cov_inv <- MASS::ginv(xcov)
  (cov_inv %*% mu_vec) /
    (matrix(1, ncol = length(hc$order), nrow = 1) %*% cov_inv %*% mu_vec)[1]
}


#' @title Calculate Peak to Trough Drawdowns
#' @param x xts object
#' @return xts of drawdown time-series
#' @export
calc_drawdown <- function(x) {

  x <- na.omit(x)
  dd <- apply(x, 2, .drawdown)
  xts(dd, as.Date(zoo::index(rownames(dd)), origin = '1970-01-01'))
}


#' @title Drawdown utility function
#' @param x xts object
#' @export
.drawdown <- function(x) {

  wi <- cumprod(x + 1)
  wi_peak <- cummax(wi)
  wi / wi_peak - 1
}


#' @title Calculate Worst Drawdown
#' @param x xts
#' @return vector of worst drawdowns for each column in `x`
#' @export
calc_max_drawdown <- function(x) {
  dd <- calc_drawdown(x)
  apply(dd, 2, min)
}


#' @title Find all the drawdowns of a time-series
#' @param x univariate xts, if more than one column is passed only the first
#'   column will be used
#' @return data.table with all the drawdowns, fields include StartDate,
#'    EndDate (recovery), TroughDate, Trough, DaysToTrough, DaysToRecover, and
#'    TotalDays
#' @export
find_drawdowns <- function(x) {

  if (ncol(x) > 1) {
    warning('x needs to be univariate, taking the first column')
    x <- x[, 1]
  }
  dd <- calc_drawdown(x)
  dd <- xts_to_dataframe(dd)
  colnames(dd) <- c('Date', 'Drawdown')
  dd$isDown <- dd[, 2] < 0
  dd$isDownLag <- c(NA, dd[1:(nrow(dd) - 1), 'isDown'])
  dd$start <- dd$isDown & !dd$isDownLag # change from 0 to negative signals drawdown start
  dd$end <- !dd$isDown & dd$isDownLag # change from negative back to 0 signals recovery
  start_date <- dd[dd$start, 1]
  end_date <- dd[dd$end, 1]
  # if lengths of start and end dates are the same the time-series ends on
  # a drawdown and we need to adjust last end date to last start date
  if (length(end_date) == length(start_date)) {
    dd_date <- data.frame(StartDate = start_date,
                          EndDate = c(end_date[2:length(end_date)],
                                      start_date[length(start_date)]))
  } else {
    dd_date <- data.frame(StartDate = start_date,
                          EndDate = end_date[2:length(end_date)])
  }

  .trunc_df <- function(df, date_start = NULL, date_end = NULL) {

    colnames(df)[1] <- 'Date'
    if (!is.null(date_start)) {
      ind <- df$Date >= as.Date(date_start)
      df <- df[ind, ]
    }
    if (!is.null(date_end)) {
      ind <- df$Date <= as.Date(date_end)
      df <- df[ind, ]
    }
    return(df)
  }

  # create unique list of drawdowns
  dd_list <- mapply(.trunc_df,
                    df = list(dd[, 1:2]),
                    date_start = dd_date$StartDate,
                    date_end = dd_date$EndDate,
                    SIMPLIFY = FALSE)
  .get_min_dd <- function(x) {
    # find trough by finding which observation equals the minimum
    indx <- which(x[, 2] == min(x[, 2]))
    # if there are more than one observations equal to the minimum take the first one
    if (length(indx) > 1) {
      indx <- indx[1]
    }
    x[indx, ]
  }
  trough_list <- lapply(dd_list, .get_min_dd)
  trough_df <- do.call(rbind, trough_list)
  res <- data.frame(StartDate = dd_date$StartDate,
                    TroughDate = trough_df$Date,
                    EndDate = dd_date$EndDate,
                    Trough = trough_df$Drawdown,
                    DaysToTrough = trough_df$Date - dd_date$StartDate,
                    DaysToRecover = dd_date$EndDate - trough_df$Date,
                    TotalDays = dd_date$EndDate - dd_date$StartDate)
  colnames(res)[4] <- 'Drawdown'
  return(res)
}


#' @title Calculate excess return (a - b)
#' @param a xts object to transform into excess return
#' @param b univariate xts object of benchmark or risk-free rate to subtract
#'   from `a`
#' @export
excess_ret <- function(a, b) {
  if (ncol(b) > 1) {
    stop('b must be univariate')
  }
  obs <- combine_xts(b, a)
  a <- obs[, 2:ncol(obs)]
  b <- obs[, 1]
  for (i in 1:ncol(a)) {
    a[, i] <- a[, i] - b
  }
  return(a)
}


#' @title Calculate geometric return
#' @param x xts object
#' @param period days, weeks, months, quarters to annualize return
#' @return vector of geometric returns
#' @export
calc_geo_ret <- function(x, period) {

  a <- freq_to_scaler(period)
  obs <- nrow(x)
  apply(x + 1, 2, prod)^(a / obs) - 1
}


#' @title Calculate Volatiltiy
#' @param x xts object
#' @param period days, weeks, months, quarters to annualize volatiltiy
#' @return vector of volatility
#' @export
calc_vol <- function(x, period) {

  a <- freq_to_scaler(period)
  apply(x, 2, sd) * sqrt(a)
}


#' @title Calculate Sharpe Ratio
#' @param x xts object
#' @param rf xts with risk-free time-series
#' @param period days, weeks, months, quarters to annaulize return and risk
#' @note
#' The function doesn't test to see if `x` and `rf` are for intersecting
#' time periods.
#' @return vector of Sharpe Ratios
#' @export
calc_sharpe_ratio <- function(x, rf, period) {

  x_geo <- calc_geo_ret(x, period)
  rf_geo <- calc_geo_ret(rf, period)
  x_vol <- calc_vol(x, period)
  (x_geo - rf_geo) / x_vol
}


#' @title Calculate Downside Volatiltiy
#' @param x xts object
#' @param period days, weeks, months, quarters to annualize volatiltiy
#' @param mar default `0`, sets min acceptable return to subset downside volatility
#' @details
#' Downside volatiltiy is the standard devation of returns for the sub-set of
#' the time-series below the `mar`
#' @return vector of downside volatility
#' @export
calc_down_vol <- function(x, period, mar = 0) {

  a <- freq_to_scaler(period)
  res <- rep(NA, ncol(x))
  for (i in 1:ncol(x)) {
    res[i] <- sd(x[x[, i] < mar, i]) * sqrt(a)
  }
  return(res)
}


#' @title Calculate Sortino Ratio
#' @param x xts object
#' @param rf xts with risk-free time-series
#' @param mar default `0`, sets min acceptable return to subset downside volatility
#' @param period days, weeks, months, quarters to annaulize return and risk
#' @note
#' The function doesn't test to see if `x` and `rf` are for intersecting
#' time periods.
#' @return vector of Sortino Ratios
#' @export
calc_sortino_ratio <- function(x, rf, period, mar = 0) {

  x_geo <- calc_geo_ret(x, period)
  rf_geo <- calc_geo_ret(rf, period)
  x_vol <- calc_down_vol(x, period, mar)
  (x_geo - rf_geo) / x_vol
}


#' @title Calculate Batting Average
#' @details
#' Average up day / Average down day, or week, month, etc
#' @param x xts
#' @param mar sets place to sub-set up / down, default is `0`
#' @export
calc_batting_avg <- function(x, mar = 0) {

  x_up <- rep(NA, ncol(x))
  x_down <- x_up
  for (i in 1:ncol(x)) {
    x_up[i] <- mean(x[x[, i] >= mar, i])
    x_down[i] <- mean(x[x[, i] < mar, i])
  }
  x_up / abs(x_down)
}


#' @title Calculate Upside Capture
#' @details
#' Geometric return of manager / geometric return of benchmark when benchmark is up
#' @param mgr xts object of manager / asset / security / etc to calculate upside capture
#' @param bench xts object of benchmark
#' @param mar sets place to sub-set up / down, default is `0`
#' @return vector of upside capture
#' @export
calc_up_capture <- function(mgr, bench, period, mar = 0) {

  mgr_up <- mgr[bench >= mar, ]
  bench_up <- bench[bench >= mar, ]
  mgr_geo <- colMeans(mgr_up)
  bench_geo <- colMeans(bench_up)
  mgr_geo / bench_geo
}


#' @title Calculate Downside Capture
#' @details
#' Geometric return of manager / geometric return of benchmark when benchmark is up
#' @param mgr xts object of manager / asset / security / etc to calculate downside capture
#' @param bench xts object of benchmark
#' @param mar sets place to sub-set up / down, default is `0`
#' @return vector of downside capture
#' @export
calc_down_capture <- function(mgr, bench, period, mar = 0) {

  mgr_down <- mgr[bench < mar, ]
  bench_down <- bench[bench < mar, ]
  mgr_geo <- colMeans(mgr_down)
  bench_geo <- colMeans(bench_down)
  mgr_geo / bench_geo
}


#' @title Calculate Univariate Beta
#' @param mgr xts manager(s)
#' @param bench xts benchmark
#' @param rf xts risk-free
#' @return vector of betas to benchmark
#' @export
calc_uni_beta <- function(mgr, bench, rf) {

  combo <- cbind(mgr, bench)
  er <- excess_ret(combo, rf)
  xcov <- cov(er)
  # covariance / variance
  xcov[, ncol(xcov)] / xcov[nrow(xcov), ncol(xcov)]
}



#' @export
up_beta <- function(mgr, bench, rf, mar = 0) {

  is_up <- bench >= mar
  calc_uni_beta(mgr[is_up, ], bench[is_up, ], rf[is_up, ])
}


#' @export
down_beta <- function(mgr, bench, rf, mar = 0) {

  is_down <- bench < mar
  calc_uni_beta(mgr[is_down, ], bench[is_down, ], rf[is_down, ])
}


#' @export
cov_ewma <- function(ret, lambda = NULL) {
  if (is.null(lambda)) {
    freq <- xts::periodicity(ret)
    lambda <- 1 - 2 / (freq_to_scaler(freq$units))
  }
  n_obs <- nrow(ret)
  cov_mat <- cov(ret)
  mu <- colMeans(ret)
  ret_centered <- sweep(ret, 2, mu, '-')
  for (obs in 1:n_obs) {
    r <- ret_centered[obs, ]
    rr <- t(r) %*% r
    cov_mat <- (1 - lambda) / (1 + lambda^n_obs) * rr + lambda * cov_mat
  }
  return(cov_mat)
}


#' @export
absorp_ratio <- function(xcor, n_pc = NULL) {

  if (is.null(n_pc)) {
    n_pc <- ceiling(nrow(xcor) / 5)
  }
  eig_res <- eigen(xcor)
  sum(eig_res$values[1:2]) / sum(eig_res$values)
}


#' @export
risk_par_wgt <- function(ret, sigma) {

  # F(y), system of nonlinear equations
  eval_f <- function(x, sigma, lambda) {
    x <- as.vector(x)
    x <- x[1:nrow(sigma)]
    f0 <- matrix(nrow = (nrow(sigma) + 1), ncol = 1)
    f0[1:nrow(sigma), 1] <- (sigma %*% x) - (lambda * 1 / x)
    f0[(nrow(sigma) + 1), 1] <- sum(x) - 1
    return(f0)
  }

  # Jacobian matrix of F(y)
  jacob_f <- function(x, sigma, lambda) {
    x <- as.vector(x)
    x <- x[1:nrow(sigma)]
    g <- matrix(nrow = (nrow(sigma) + 1), ncol = (ncol(sigma) + 1))
    g[1:nrow(sigma), 1:ncol(sigma)] <- sigma + as.vector(lambda) *
      diag(1 / x^2)
    g[1:nrow(sigma), (ncol(sigma) + 1)] <- 1 / x
    g[(nrow(sigma) + 1), 1:ncol(sigma)] <- rep(1, ncol(sigma))
    g[(nrow(sigma) + 1), (ncol(sigma) + 1)] <- 0
    return(g)
  }

  # set stopping points for max iterations if no solution
  # and tolerance for solution
  max_iter <- 100
  tol <- 1e-6

  # establish list for ERP weights to go into and set
  # number of assets

  n_assets <- ncol(ret)
  n_obs <- nrow(ret)
  for(i in 1:max_iter){
    if (i == 1){
      # 1/N first weight guess
      next_x <- c(rep(1/n_assets, n_assets), 1)
      lambda <- t(next_x[1:n_assets]) %*% sigma %*%
        next_x[1:n_assets] / n_assets
    }
    else{
      j_inv <- solve(jacob_f(next_x, sigma, lambda))
      f_mat <- eval_f(next_x, sigma, lambda)
      ans <- next_x - j_inv %*% f_mat
      if(norm(next_x - ans) <= tol){
        break
      }
      next_x <- ans
      lambda <- t(next_x[1:n_assets,]) %*% sigma %*%
        next_x[1:n_assets,] / n_assets
    }
  }
  return(ans[1:n_assets])
}


#' @export
boot_strap <- function(x, n) {
  is_vec <- is.null(dim(x))
  if (is_vec) {
    n_row <- length(x)
  } else {
    n_row <- nrow(x)
  }
  rand_row <- round(runif(n, 1, n_row), 0)
  if (is_vec) {
    return(x[rand_row])
  } else {
    return(x[rand_row, ])
  }
}


#' @export
block_boot_strap <- function(x, n, block) {
  x <- as.vector(x)
  boot_length <- ceiling(n / block)
  boot_vec <- rep(NA, block * boot_length)
  rand_row <- round(runif(boot_length, 0, length(x) - block + 1), 0)
  j <- 1
  for (i in 1:boot_length) {
    rand_block <- rand_row[i]:(rand_row[i] + (block - 1))
    boot_vec[j:(j + (block - 1))] <- x[rand_block]
    j <- j + block
  }
  boot_vec[1:n]
}

#' @export
te_min_qp <- function(fund, fact, force_pd = FALSE) {

  n_fact <- ncol(fact)
  cov_fact <- cov(fact)
  if (force_pd) {
    cov_fact <- Matrix::nearPD(cov_fact)[[1]]
  }
  cov_vec <- matrix(nrow = n_fact, ncol = 1)
  for (i in 1:n_fact) {
    cov_vec[i, 1] <- cov(fact[, i], fund)
  }
  a_mat_t <- rbind(rep(1, n_fact), diag(-1, n_fact), diag(1, n_fact))
  a_mat <- t(a_mat_t)
  b_0 <- c(1, rep(-1, n_fact), rep(0, n_fact))
  res <- quadprog::solve.QP(cov_fact, cov_vec, a_mat, bvec = b_0, meq = 1)
  return(res)
}

#' @export
sig_group_sim <- function(ret) {

  xcor <- cor(ret)
  lambda <- eigen(xcor)$values

  noise_lambda <- matrix(nrow = 1000, ncol = length(lambda))
  for (i in 1:1000) {
    noise_ret <- apply(ret, 2, sample, size = nrow(ret), replace = FALSE)
    noise_lambda[i, ] <- eigen(cor(noise_ret))$values
  }
  noise_95 <- apply(noise_lambda, 2, quantile, probs = 0.95)
  return(sum(lambda > noise_95))
}

#' @export
run_cluster <- function(ret, k_group) {

  clv_res <- ClustVarLV::CLV(ret, method = 2)
  group_res <- summary(clv_res, k_group)
  group_nm <- rownames(group_res$groups[[1]])
  if (k_group > 1) {
    for (i in 2:k_group) {
      group_nm <- c(group_nm, rownames(group_res$groups[[i]]))
    }
  }
  p <- psych::pca(ret[, group_nm], k_group)
  p2 <- psych::pca(ret, k_group)
  group_df <- data.frame(Manager = colnames(ret),
                         Group = ClustVarLV::get_partition(clv_res, k_group),
                         P1 = p2$loadings[, 1],
                         P2 = p2$loadings[, 2])
  group_df$Group <- factor(group_df$Group, unique(group_df$Group))
  g_biplot <- ggplot(group_df, aes(x = P2, y = P1, color = Group, label = Manager)) +
    geom_point() +
    geom_segment(aes(x = 0, y = 0, xend = P2, yend = P1)) +
    ggrepel::geom_text_repel(size = 3) +
    xlab('Component 2 Loading') + ylab('Component 1 Loading') +
    theme(legend.position = 'none')
  p_loadings <- data.frame(Manager = rownames(p$loadings), p$loadings[,])
  p_load_group <- merge(p_loadings, group_df[, c('Manager', 'Group')])
  plotdf <- tidyr::pivot_longer(p_load_group, -one_of('Manager', 'Group'))
  plotdf$Group <- factor(plotdf$Group, unique(plotdf$Group))
  plotdf$Manager <- factor(plotdf$Manager, unique(plotdf$Manager))
  plotdf$name <- factor(plotdf$name, unique(plotdf$name))
  plotdf <- plotdf[order(plotdf$Group, decreasing = TRUE), ]
  g_load <- ggplot(plotdf, aes(x = Manager, y = value, fill = Group)) +
    scale_x_discrete(limits = unique(plotdf$Manager)) +
    geom_bar(stat = 'identity', position = 'dodge') +
    coord_flip() +
    facet_wrap(. ~ name) +
    theme(legend.position = 'none')
  res <- list()
  res$biplot <- g_biplot
  res$loadings <- g_load
  return(res)
}

#' @export
clean_ret_na_col <- function(ret, thresh = .05, comm_start = TRUE, 
                             comm_end = TRUE) {

  if (comm_start) {
    first_date <- first_comm_start(ret)
    ret <- cut_time(x, first_date)
  }
  if (comm_end) {
    last_date <- last_comm_end(ret)
    ret <- cut_time(x, last_date)
  }
  thresh_num <- floor(thresh * nrow(ret))
  bad_col <- rep(FALSE, ncol(ret))
  for (i in 1:ncol(ret)) {
    miss <- sum(is.na(ret[, i]))
    if (miss > thresh_num) {
      bad_col[i] <- TRUE
    } else {
      ret[miss, i] <- 0
    }
  }
  ret_clean <- ret[, !bad_col]
  return(ret_clean)
}

#' @export
daily_spline <- function(d_ret, m_ret, thresh = 0.0000001) {
  m_dt <- zoo::index(m_ret)
  m_dt <- substr(m_dt, 1, 7)
  d_adj <- d_ret
  for (m in 1:nrow(m_ret)) {

    dt_ix <- paste0(m_dt[m], '/', m_dt[m])
    di <- d_ret[dt_ix]
    mi <- m_ret[m_dt[m]]

    lx <- -0.01
    ux <- 0.011
    for (i in 1:1000) {

      mx <- (lx + ux) / 2

      if (abs(.test_diff(di + mx, mi)) < thresh) {
        break
      }

      if (.test_diff(di + mx, mi) > 0) {
        ux <- mx
      } else {
        lx <- mx
      }
    }
    d_adj[dt_ix] <- d_ret[dt_ix] + mx
  }
  return(d_adj)
}

#' @export
.test_diff <- function(a, b) {
  (prod(a+1)-1) - b
}


#' @export
summary_stats <- function(fund, bench, rf, period) {

  n_funds <- ncol(fund)
  combo <- na.omit(cbind(fund, bench, rf))
  a <- freq_to_scaler(period)
  if (nrow(combo) > a) {
    geo_ret <- calc_geo_ret(combo[, 1:(n_funds+1)], period)
  } else {
    geo_ret <- apply(combo[, 1:(n_funds+1)] + 1, 2, prod) - 1
  }
  xcov <- cov(combo[, 1:(n_funds+1)])
  vol <- sqrt(diag(xcov) * a)
  xvar <- PerformanceAnalytics::VaR(combo[, 1:(n_funds+1)]) * sqrt(a / 12)
  up_capt <- calc_up_capture(combo[, 1:(n_funds+1)], combo[, n_funds + 1], period)
  down_capt <- calc_down_capture(combo[, 1:(n_funds+1)], combo[, n_funds + 1], period)
  xbeta <- xcov[, n_funds + 1] / xcov[n_funds + 1, n_funds + 1]
  ar <- calc_geo_ret(combo[, 1:(n_funds+1)] - combo[, rep(n_funds + 1, n_funds+1)],
                     period)
  acov <- cov(combo[, 1:(n_funds+1)] - combo[, rep(n_funds + 1, n_funds+1)])
  te <- sqrt(diag(acov) * a)
  ir <- ar / te
  rf_geo <- calc_geo_ret(combo[, n_funds + 2], period)
  sharpe <- (geo_ret - rf_geo) / vol
  sortino <- calc_sortino_ratio(combo[, 1:(n_funds + 1)], combo[, n_funds + 2],
                                period)
  dd <- calc_drawdown(combo[, 1:(n_funds + 1)])
  maxdd <- apply(dd, 2, min)
  calmar <- geo_ret / -maxdd

  df <- data.frame(
    Geo.Ret = f_percent(geo_ret, 2),
    Volatility = f_percent(vol, 2),
    Beta = f_num(xbeta, 2),
    VaR.Month.95 = f_percent(as.numeric(xvar), 2),
    Max.Drawdown = f_percent(maxdd, 2),
    Sharpe.Ratio = f_num(sharpe, 2),
    Sortino.Ratio = f_num(sortino, 2),
    Calmar = f_num(calmar, 2),
    TE = f_percent(te, 2),
    Info.Ratio = f_num(ir, 2),
    Up.Capture = f_percent(up_capt, 2),
    Down.Capture = f_percent(down_capt, 2)
  )
  df <- t(df)
  colnames(df) <- c(colnames(fund), colnames(bench))
  return(df)
}
