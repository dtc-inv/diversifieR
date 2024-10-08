% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wrangle_holdings.R
\name{merge_fina_rec}
\alias{merge_fina_rec}
\title{Merge financial data to holdings data frame}
\usage{
merge_fina_rec(mdf, bucket)
}
\arguments{
\item{mdf}{list output from merge_msl containing holdings data that has
been merged with the msl}

\item{bucket}{S3 file system from Database Object or arrow}
}
\value{
mdf list with merged financial data in the mdf$match data.frame
}
\description{
Merge financial data to holdings data frame
}
