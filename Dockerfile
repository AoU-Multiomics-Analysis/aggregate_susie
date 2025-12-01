#FROM mambaorg/micromamba:1.5.3
FROM ghcr.io/prefix-dev/pixi:latest

## Set up environment
#ENV MAMBA_DOCKERFILE_ACTIVATE=1
#ENV PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
## Create a new environment and install packages
RUN pixi global install -c dnachun -c conda-forge -c bioconda r-base=4.4 tabix google-cloud-sdk python=3.13 
RUN pixi global install --environment r-base \
    r-optparse \
    r-bedr \
    r-data.table \
    r-janitor \
    r-tidyverse=2.0.0 \
    r-arrow \
    bioconductor-plyranges \
    bioconductor-rtracklayer
#RUN micromamba install -y -n base -c conda-forge -c bioconda \
    #conda-forge::r-tidyverse=2.0.0 \
    #conda-forge::datatable \
    #conda-forge::r-optparse \
    #conda-forge::r-optparse \
    #bioconda::bioconductor-rtracklayer \
    #conda-forge::r-arrow \
    #bioconda::bioconductor-plyranges \
    #conda-forge::google-cloud-sdk \
    #conda-forge::r-bedr \
    #bioconda::tabix

COPY merge_susie.R .
COPY annotate_susie_data.R . 

CMD ["bash"]
 
