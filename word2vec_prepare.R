require(dplyr, quietly = TRUE)
source("~/r projects/mysql_io.R")

weibo_trim <- function(txt) {
  txt <- gsub(pattern = "http(s){0,1}:[a-zA-Z\\/\\.0-9]+", "", txt)
  txt <- gsub(pattern = "//@.+?:", "", txt)
  txt <- gsub(pattern = "\\#.+?\\#|【.+?】|\\[.+?\\]", "", txt)
  txt <- gsub(pattern = "我在(\\w){0,2}[:：](\\w*)", "", txt)
  txt <- gsub(pattern = "哈{3,}", "哈哈", txt)
  txt <- gsub(pattern = "[A-Za-z0-9]", "", txt)
  txt <- gsub(pattern = ",NA", "", txt)
  return(txt)
}

text_segmenter <- function(target_text){
  require(jiebaR, quietly = TRUE)
  require(parallel, quietly = TRUE)
  n_cores <- get_cores()
  cc <- worker(stop_word = "/home/jeffmxh/stopwords_utf8.txt")
  segment_list <- mclapply(1:length(target_text), function(i){
    seg_list <- tryCatch(
      {
        cc[target_text[i]]
      },
      error = function(e){seg_list <- c()},
      warning = function(w){seg_list <- c()}
    )
    return(seg_list)
  },mc.cores = n_cores
  )
  return(segment_list)
}

target_data <- get_db_data("SELECT * FROM weibo_raw_data")
target_data$content <- paste(target_data$content, target_data$retweeted_content, sep = ",")
target_data$content <- weibo_trim(as.character(target_data$content))
target_data <- target_data %>% filter(nchar(content)>10)
seg_list <- text_segmenter(target_data$content)
seg_bind_list <- mclapply(seg_list, function(seg){return(paste0(seg, collapse = " "))}, mc.cores = 16)


for(i in 1:length(seg_bind_list)){
  cat(seg_bind_list[[i]], "\n", file = "/home/jeffmxh/word2vec/weibo_seg.txt", append = TRUE)
  if(i%%20000==0){
    cat(i ," finished!\n")
  }
}
