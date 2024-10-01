#' @export
check_reb_freq <- function(x) {
  x <- x[1]
  if (is.null(x)) {
    return(x)
  }
  if (x %in% c('BH', 'D', 'M', 'Q', 'A')) {
    return(x)
  }
  stop(paste0(x, ' not in BH, D, M, Q, A'))
}

#' @title Utility function to check if an object has dimensions
#' @return TRUE if it has dimensions, FALSE if not
#' @export
is_null_dim <- function(x) {
  if (is.null(dim(x))) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}
