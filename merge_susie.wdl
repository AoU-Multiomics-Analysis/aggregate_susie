version 1.0



task AggregateSusie{
    input{
        #Array[File] SusieParquets
        File SusieParquetsFOFN
        Int Memory
        String OutputPrefix
        Int NumThreads 
    }

    command <<<
    export GSUTIL_PARALLEL_PROCESS_COUNT=32
    export GSUTIL_PARALLEL_THREAD_COUNT=8

    awk '{print $1}' ~{SusieParquetsFOFN} | grep -v '^$' > file_paths.txt 

    mkdir -p localized
    gsutil -m cp -I localized/ < file_paths.txt 

    # Write the new local file paths into filelist.txt
    ls -1 "$(pwd)/localized/*" > filelist.txt
    Rscript /tmp/merge_susie.R --FilePaths file_paths.txt  --OutputPrefix ~{OutputPrefix}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/aggregate_susie:main"
        disks: "local-disk 500 SSD"
        memory: "~{Memory}GB"
        cpu: "~{NumThreads}"
    }
 


    output {
        File MergedSusieParquet = "${OutputPrefix}_SusieMerged.parquet" 
        File MergedSusieTsv = "${OutputPrefix}_SusieMerged.tsv.gz" 
    } 

}


task AnnotateSusie {
    input {
        File SusieParquet 
        File GencodeGTF
        File PlinkAfreq
        String OutputPrefix
        Int Memory
        File AnnotationENCODE 
        File AnnotationFANTOM5 
        File AnnotationVEP 
        File AnnotationGnomad 
        File AnnotationPhyloP 
      

    }
    command <<<
    Rscript /tmp/annotate_susie_data.R \
        --OutputPrefix ~{OutputPrefix} \
        --GencodeGTF ~{GencodeGTF} \
        --PlinkAfreq ~{PlinkAfreq} \
        --SusieParquet ~{SusieParquet} \
        --phyloPBigWig ~{AnnotationPhyloP} \
        --FANTOM5 ~{AnnotationFANTOM5} \
        --gnomadConstraint ~{AnnotationGnomad} \
        --ENCODEcCRES ~{AnnotationENCODE} \
        --VEPAnnotationsTable ~{AnnotationVEP}
    >>>
   runtime {
        docker: "ghcr.io/aou-multiomics-analysis/aggregate_susie:main"
        disks: "local-disk 500 SSD"
        memory: "~{Memory}GB"
        cpu: "1"
    }


    output {
        File AnnotatedSusieParquetOut = "~{OutputPrefix}_SusieMerged.annotated.parquet" 
    }

}



workflow AggregateSusieWorkflow {
    input {
        #Array[File] SusieParquets
        File SusieParquetsFOFN
        Int Memory 
        String OutputPrefix
        Int NumThreads

        File GencodeGTF 
        File PlinkAfreq
        File AnnotationPhyloP 
        File AnnotationENCODE 
        File AnnotationFANTOM5 
        File AnnotationVEP 
        File AnnotationGnomad 
    }
    
    call AggregateSusie {
        input:
            SusieParquetsFOFN = SusieParquetsFOFN,
            OutputPrefix = OutputPrefix,
            Memory = Memory,
            NumThreads = NumThreads
    }

    call AnnotateSusie {
        input:
            SusieParquet = AggregateSusie.MergedSusieParquet,
            GencodeGTF = GencodeGTF,
            PlinkAfreq = PlinkAfreq,
            OutputPrefix = OutputPrefix,
            Memory = Memory,
            AnnotationPhyloP = AnnotationPhyloP,
            AnnotationENCODE = AnnotationENCODE,
            AnnotationFANTOM5 = AnnotationFANTOM5,
            AnnotationVEP = AnnotationVEP,
            AnnotationGnomad = AnnotationGnomad
    } 

    output {
        File AnnotatedMergedSusieParquet = AnnotateSusie.AnnotatedSusieParquetOut
        #File MergedSusieParquet = AggregateSusie.MergedSusieParquet
        #File MergedSusieTsv = AggregateSusie.MergedSusieTsv
    }

}
