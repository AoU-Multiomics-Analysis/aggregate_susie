version 1.0



task AggregateSusie{
    input{
        Array[File] SusieParquets
        Int Memory
        String OutputPrefix
    }

    command <<<
    for file in ~{sep=' ' SusieParquets}; do
       echo $file >> filelist.txt
    done

    Rscript /tmp/merge_susie.R --FilePaths filelist.txt  --OutputPrefix ~{OutputPrefix}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/aggregate_susie:main"
        disks: "local-disk 500 SSD"
        memory: "~{Memory}GB"
        cpu: "1"
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
     
    }
    command <<<
    Rscript /tmp/annotate_susie_data.R \ 
        --OutputPrefix ~{OutputPrefix} \
        --GencodeGTF ~{GencodeGTF} \
        --PlinkAfreq ~{PlinkAfreq} \
        --SusieParquet ~{SusieParquet}


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
        Array[File] SusieParquets 
        Int Memory 
        String OutputPrefix
        File GencodeGTF 
        File PlinkAfreq
    }
    
    call AggregateSusie {
        input:
            SusieParquets = SusieParquets,
            OutputPrefix = OutputPrefix,
            Memory = Memory
    }

    call AnnotateSusie {
        input:
            SusieParquet = AggregateSusie.MergedSusieParquet,
            GencodeGTF = GencodeGTF,
            PlinkAfreq = PlinkAfreq,
            OutputPrefix = OutputPrefix,
            Memory = Memory
    } 

    output {
        File AnnotatedMergedSusieParquet = AnnotateSusie.AnnotatedSusieParquetOut
        #File MergedSusieParquet = AggregateSusie.MergedSusieParquet
        #File MergedSusieTsv = AggregateSusie.MergedSusieTsv
    }

}
