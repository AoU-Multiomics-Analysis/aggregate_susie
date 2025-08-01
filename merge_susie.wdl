version 1.0



task AggregateSusie{
    input{
        Array[File] SusieParquets
        Int Memory
        String OutputPrefix
    }

    command <<<
   for file in ~{sep='\n' SusieParquets}; do
   echo $file >> filelist.txt
   done

    Rscript merge_susie.R \ 
       --FilePaths filelist.txt \
       --OutputPrefix ~{OutputPrefix}
    >>>

    runtime {
        docker: "quay.io/biocontainers/htslib:1.22.1--h566b1c6_0"
        disks: "local-disk 500 SSD"
        memory: "{Memory}GB"
        cpu: "1"
    }
 


    output {
        File MergedSusieParquet = "${OutputPrefix}_SusieMerged.parquet" 
        File MergedSusieTsv = "${OutputPrefix}_SusieMerged.tsv.gz" 
    } 

}


workflow AggregateSusieWorkflow {
    input {
        Array[File] SusieParquets 
        Int Memory 
        String OutputPrefix
    }
    
    call AggregateSusie {
        input:
            SusieParquets = SusieParquets,
            OutputPrefix = OutputPrefix,
            Memory = Memory
    }
    output {
        File MergedSusieParquet = AggregateSusie.MergedSusieParquet
        File MergedSusieTsv = AggregateSusie.MergedSusieTsv
    }

}
