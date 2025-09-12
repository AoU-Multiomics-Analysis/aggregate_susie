library(tidyverse)
library(data.table)
library(rtracklayer)
library(arrow)
library(plyranges)


########## LOADING FUNCTIONS ##################

load_constraint_data <- function(gnomad_constraint_data) {
message('Loading gnomad data')
gnomad_dat <- fread(gnomad_constraint_data) %>% 
            filter(canonical == TRUE) %>% 
            select(gene_id,`lof.pLI`) %>% 
            mutate(Constrained =  case_when(lof.pLI > 0.9 ~ 'Constrained',TRUE ~ 'Unconstrained')) 
gnomad_dat
}


# load encode hg38 regulatory element annotations
load_ENCODE_data <- function(ENCODE_cCRES_path) {
message('Loading ENCODE data')
ENCODE_cres <- fread(ENCODE_cCRES_path) %>% 
    dplyr::rename('seqnames' =1,'start' =2,'end' =3,'type' =6)  %>% 
    separate_rows(type,sep = ',') %>% 
    mutate(value = TRUE) %>% 
    pivot_wider(names_from = type,values_from = value,values_fill = FALSE)
ENCODE_cres

}


# load plink allele frequency data
load_afreq_data <- function(afreq_path){
message('Loading allele frequency data')
dat <- fread(afreq_path) %>% 
        dplyr::rename('variant' = 'ID') %>% 
        mutate(variant = str_replace(variant,':','_')) %>% 
        mutate(chrom = case_when(str_detect(variant,'chrchr') ~ str_remove(variant,'chr'),TRUE ~ variant)) %>% 
        dplyr::select(variant,ALT_FREQS)
 
dat     
    
}

# function to load in FANTOM5 data, note this data 
# as of right now is downloaded from the UCSC genome browser
load_FANTOM5_data <- function(FANTOM5_path) {
message('Loading FANTOM5 data')
FANTOM5_df <- fread(FANTOM5_path) %>% 
    dplyr::rename('seqnames' ='V1','start' = 'V2','end' ='V3') %>% 
    mutate(FANTOM5 = TRUE) %>% 
    select(seqnames:end,FANTOM5) %>% 
    makeGRangesFromDataFrame(keep.extra = TRUE)
FANTOM5_df
}


############### QUERY FUNCTIONS #################

# helper function to query tabix data from vep. 
# This is not the vep generated VCF but rather 
# a cleaned table that has typically been summarized 
# to the worst consequence
query_tabix <- function(fm_res,tabix_path) {
ranges <- fm_res %>% 
    transmute(variant = paste0(seqnames,':',start-1,'-',start)) %>% 
    pull(variant)
tabix_res <- tabix(ranges,tabix_path) %>% 
        distinct()  %>%
        separate_rows(V5,sep='&') %>%  
        mutate(value = TRUE) %>% 
        mutate(V5 = str_remove_all(V5, "\\s+")) %>% 
        pivot_wider(names_from = V5,values_from = value,values_fill = FALSE) %>% 
        dplyr::rename('seqnames' = 'V1','start' = 'V2','ref' = 'V3','alt' = 'V4')
tabix_res
}  

# helper function to query big wig file and 
# get scores
query_bigwig <- function(fm_data,bw_path) {
message(paste0('Annotating with ',basename(bw_path)))

positions <- fm_data %>% makeGRangesFromDataFrame()
bw <- BigWigFile(bw_path)
message('importing big wig')
phyloP_scores <- import(bw, selection = BigWigSelection(positions))

hits <- findOverlaps(positions, phyloP_scores)
scores <- rep(NA_real_, length(positions))
scores[queryHits(hits)] <- mcols(phyloP_scores)$score[subjectHits(hits)]
fm_data$phylop <- scores
fm_data
}


# querys vep table and removes synonymous variant 
# annotations if another column is  true 
query_vep_table <- function(fm_data,VEP_table) {
message('Annotating with VEP')
VEP_annotation_data <- query_tabix(fm_data ,VEP_table)

VEP_annotated <- fm_data %>% 
    mutate(start = as.character(start)) %>% 
    left_join(VEP_annotations,by = c('seqnames','start','ref','alt')) %>% 
    mutate(
     synonymous_variant = ifelse(
      # check if ANY logical column other than keep is TRUE
      if_any(where(is.logical) & !matches("^synonymous_variant$"), ~ .x),
      FALSE,
      synonymous_variant
        )
      )
VEP_annotated
}

# helper function to annotate fine mapped data with granges 
# object
query_grange_data <- function(fm_data,grange_annotations) {
annotated_fm_data <- fm_data %>% 
    mutate(start = position,end = position) %>% 
    mutate(chromosome = paste0('chr',chromosome)) %>% 
    makeGRangesFromDataFrame(keep.extra = TRUE) %>% 
    join_overlap_left(grange_annotations) %>% 
    data.frame() %>% 
    mutate(across(where(is.logical), ~replace_na(., FALSE)))
annotated_fm_data

}



########### COMMAND LINE ARGUMENTS ########
message('Begin')
option_list <- list(
  optparse::make_option(c("--SusieParquet"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--GencodeGTF"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--OutputPrefix"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--PlinkAfreq"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--ENCODEcCRES"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--VEPAnnotationsTable"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--gnomadConstraint"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--phyloPBigWig"), type="character", default=NULL, metavar = "type"),
  optparse::make_option(c("--FANTOM5"), type="character", default=NULL, metavar = "type")

)

message('Parsing command line arguments')
opt <- optparse::parse_args(optparse::OptionParser(option_list=option_list))

OutputPrefix <- opt$OutputPrefix


annotated_parquet <- paste0(opt$OutputPrefix,'_SusieMerged.annotated.parquet') 
message(paste0('Writing to:',annotated_parquet))

# this file needs to be a parsed version of the a VEP consequences VCF
PathVEP <- opt$VEPAnnotationsTable
PathENCODE <- opt$ENCODEcCRES
PathPhyloP <- opt$phyloPBigWig
PathGnomad <- opt$gnomadConstraint
PathFANTOM5 <- opt$FANTOM5
PathSusie <- opt$SusieParquet

########### LOAD DATA ############


FANTOM5_granges <- load_FANTOM5_data(PathFANTOM5)
ENCODE_data <- load_ENCODE_data(PathENCODE)
gnomad_data <- load_constraint_data(PathGnomad) 

allele_frequencies <- load_afreq_data(opt$PlinkAfreq)

#susie_res <- load_finemapping_data(opt$SusieParquet)


message('Loading GTF')
gene_data <- rtracklayer::readGFF(opt$GencodeGTF) %>% filter(type == 'gene')

message('Extracting TSS  locations')
tss_data <- gene_data %>% mutate(tss = case_when(strand == '+' ~ start,TRUE ~ end)) %>% 
            dplyr::select(seqid,tss,gene_id,gene_type,gene_name) %>% 
            mutate(start = tss,end = tss ) %>% 
             makeGRangesFromDataFrame(keep.extra = TRUE)



# annotate fine mapping data with internal data 
# generated from QTL mapping
message('Annotating fine-mapping data')
annotated_fm_res <-  arrow::read_parquet(PathSusie) %>%
  mutate(group = OutputPrefix) %>% 
  left_join(allele_frequencies,by = 'variant' ) %>% 
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
    )  %>% 
    dplyr::select(-ALT_FREQS ) %>%
    mutate(PIP_decile = cut(pip, breaks = seq(0, 1, by = 0.1), include.lowest = TRUE)) %>%  
    mutate(gene_id = str_remove(molecular_trait_id,'\\..*')) 


message('Annotating fine-mapping data with external data sources')
full_annotated_data <- annotated_fm_res %>%
            query_grange_data(ENCODE_data) %>% 
            query_grange_data(FANTOM5_granges) %>% 
            query_vep_table(PathVEP)  %>%
            query_bigwig(PathPhyloP)
            left_join(gnomad_data,by = 'gene_id')  

message('Cleaning annotated data')
cleaned_full_annotated_data <- full_annotated_data %>% 
    mutate(
     synonymous_variant = ifelse(
      # check if ANY logical column other than keep is TRUE
      if_any(where(is.logical) & !matches("^synonymous_variant$"), ~ .x),
      FALSE,
      synonymous_variant
        )
    ) %>% 
  mutate(Annotation = case_when(dELS == TRUE ~ 'Enhancer', pELS == TRUE ~ 'Enhancer',PLS == TRUE ~ 'Promoter',FANTOM5 == TRUE ~ 'Promoter')) 



message('Writing to output') 
cleaned_full_annotated_data %>% arrow::write_parquet(annotated_parquet)
 
