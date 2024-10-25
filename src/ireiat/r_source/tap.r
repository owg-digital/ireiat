# import needed libraries
library(arrow)
library(igraph)
library(cppRouting)

# set parallelization needed for RcppParallel
RcppParallel::setThreadOptions(numThreads = parallel::detectCores())

# read command-line args
args <- commandArgs(trailingOnly = TRUE)
network_file <- args[1]
od_file <- args[2]
max_gap <- as.numeric(args[3])
output_file <- args[4]
max_iterations <- as.numeric(args[5])

# print data about read files
od_df <- read_parquet(od_file)
print(sprintf("Number of rows in O-D file: %s", nrow(od_df)))

network_df <- read_parquet(network_file)
print(sprintf("Number of rows in network file: %s", nrow(network_df)))

sgr <- makegraph(df = network_df[,c("tail", "head", "fft")],
                 directed = TRUE,
                 capacity = network_df$capacity,
                 alpha = network_df$alpha,
                 beta = network_df$beta)

# traffic <- get_aon(Graph = sgr, from = od_df$from, to = od_df$to, demand = od_df$tons)
# head(traffic)
traffic <- assign_traffic(Graph = sgr,
                          from = od_df$from,
                          to = od_df$to,
                          demand = od_df$tons,
                          max_gap = max_gap,
                          algorithm = "dial",
                          verbose = TRUE,
                          aon_method="d",
                          max_it=max_iterations)
print("Successfully solved.")
output_path <- file.path(output_file)
# write_parquet(traffic,output_path)
write_parquet(traffic$data,output_path)
print(sprintf("Written to %s", output_path))
