library(tidyverse)
library(data.table)
library(rtracklayer)
library(arrow)
library(plyranges)

load_afreq_data <- function(afreq_path){
#system(paste0('gsutil cp ',afreq_path , ' .'))

#split_name <- str_split(basename(afreq_path),'_|\\.') %>% unlist()
#group <- split_name[3]
dat <- fread(afreq_path) %>% 
        dplyr::rename('variant' = 'ID') %>% 
        mutate(variant = str_replace(variant,':','_'))
  #      mutate(group  = group ) %>% 
        #select(-1)  
dat     
    
}


load_finemapping_data <- function(path){
   
fm_data <- arrow::read_parquet(path)  %>% 
    #separate(variant,into = c('chrom','pos','alt')) %>%
    #mutate(chrom = case_when(str_detect(chrom,'chrchr') ~ str_remove(chrom,'chr'),TRUE ~ chrom)) %>% 
    #extract(pos, into = c("pos", "ref"), regex = "([0-9]+)([A-Za-z]+)") %>% 
    mutate(variant = paste(paste0('chr',chromosome),position,ref,alt,sep='_'))
fm_data
    
}


########### COMMAND LINE ARGUMENTS ########
message('Begin')
option_list <- list(
  optparse::make_option(c("--SusieParquet"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--GencodeGTF"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--OutputPrefix"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--PlinkAfreq"), type="character", default=NULL, metavar = "type")
)

message('Parsing command line arguments')
opt <- optparse::parse_args(optparse::OptionParser(option_list=option_list))

OutputPrefix <- opt$OutputPrefix


annotated_parquet <- paste0(opt$OutputPrefix,'_SusieMerged.annotated.parquet') 
message(paste0('Writing to:',annotated_parquet))

########### LOAD DATA ############


message('Loading allele frequencies')
allele_frequencies <- load_afreq_data(opt$PlinkAfreq)

message('Loading finemapping')
susie_res <- load_finemapping_data(opt$SusieParquet)


message('Loading GTF')
gene_data <- rtracklayer::readGFF(opt$GencodeGTF) %>% filter(type == 'gene')

message('Extracting TSS  locations')
tss_data <- gene_data %>% mutate(tss = case_when(strand == '+' ~ start,TRUE ~ end)) %>% 
            dplyr::select(seqid,tss,gene_id,gene_type,gene_name) %>% 
            mutate(start = tss,end = tss ) %>% 
             makeGRangesFromDataFrame(keep.extra = TRUE)



message('Annotating fine-mapping data')
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
    left_join(tss_data %>% data.frame() %>% select(-seqnames,-start,-end,-width,-strand) ,by = 'gene_id')  %>% 
    mutate(distTSS = as.numeric(position) - as.numeric(tss),
           PIP_bin = cut(pip,breaks = 5)
    )  


message('Writing to output') 
annotated_fm_res %>% arrow::write_parquet(annotated_parquet)
 
