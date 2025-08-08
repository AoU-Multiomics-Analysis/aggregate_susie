library(tidyverse)
library(data.table)
library(rtracklayer)
library(arrow)
library(plyranges)

load_afreq_data <- function(afreq_path){
#system(paste0('gsutil cp ',afreq_path , ' .'))

#split_name <- str_split(basename(afreq_path),'_|\\.') %>% unlist()
#group <- split_name[3]
dat <- fread(afreq_path) 
  #      mutate(group  = group ) %>% 
        #select(-1)  
dat     
    
}


load_finemapping_data <- function(path){
   
fm_data <- arrow::read_parquet(path) %>% 
    separate(variant_id,into = c('chrom','pos','alt')) %>% 
    extract(pos, into = c("pos", "ref"), regex = "([0-9]+)([A-Za-z]+)") 
fm_data
    
}


########### COMMAND LINE ARGUMENTS ########
option_list <- list(
  #TODO look around if there is a package recognizing delimiter in dataset
  optparse::make_option(c("--SusieParquet"), type="character", default=NULL,
                        help="Phenotype metadata file path of genes used in expression-matrix. Tab separated", metavar = "type"),
  optparse::make_option(c("--GencodeGTF"), type="character", default=null,
                        help="sample metadata file path of genes used in expression-matrix. tab separated", metavar = "type"),
  optparse::make_option(c("--OutputPrefix"), type="character", default=null,
                        help="sample metadata file path of genes used in expression-matrix. tab separated", metavar = "type"),
  optparse::make_option(c("--PlinkAfreq"), type="character", default=null,
                        help="sample metadata file path of genes used in expression-matrix. tab separated", metavar = "type")
)

opt <- optparse::parse_args(optparse::OptionParser(option_list=option_list))

OutputPrefix <- opt$OutputPrefix


annotated_parquet <- paste0(opt$OutputPrefix,'_SusieMerged.annotated.parquet') 


########### LOAD DATA ############


allele_frequencies <- load_afreq_data(opt$PlinkAfreq)
susie_res <- load_finemapping_data(opt$SusieParquet)


gene_data <- rtracklayer::readGFF(opt$GencodeGTF) %>% filter(type == 'gene')
tss_data <- gene_data %>% mutate(tss = case_when(strand == '+' ~ start,TRUE ~ end)) %>% 
            dplyr::select(seqid,tss,gene_id,gene_type,gene_name) %>% 
            mutate(start = tss,end = tss ) %>% 
             makeGRangesFromDataFrame(keep.extra = TRUE)



annotated_fm_res <-  susie_res %>%
  mutate(group = OutputPrefix) %>% 
  left_join(allele_frequencies %>% select(-REF,-ALT),by = c('variant' = 'ID','group')) %>% 
  mutate(MAF = case_when(ALT_FREQS > .5 ~ 1 -ALT_FREQS,TRUE ~ ALT_FREQS)) %>% 
  mutate(
        AF_bin = case_when(
          MAF  < 0.01 ~ "rare (0.1–1%)",
          MAF >= 0.01 & MAF < 0.05  ~ "low-freq (1–5%)",
          MAF >= 0.05 ~ "common (≥5%)"
        )
      ) %>% 
    mutate(gene_id = str_remove(molecular_trait_id,'.*_'))  %>% 
    left_join(tss_data %>% data.frame()%>% select(-seqnames,-start,-end,-width,-strand) ,by = 'gene_id')  %>% 
    mutate(distTSS = as.numeric(position) - as.numeric(tss),
           PIP_bin = cut(pip,breaks = 5))  


annotated_fm_res %>% arrow::write_parquet(annotated_parquet)
 
