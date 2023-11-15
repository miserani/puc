FROM jupyter/pyspark-notebook:python-3.10.11

# Instale as bibliotecas adicionais
RUN pip install pandas scikit-learn matplotlib geopandas MarkupSafe Faker confluent-kafka folium ydata-profiling

# Definir o proprietário do diretório de trabalho
USER root
RUN chown -R 1000:100 /home/jovyan/work
RUN chmod -R 777 /home/jovyan/work
USER jovyan
