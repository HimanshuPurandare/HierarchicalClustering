# Hierarchical Clustering of Iris DataSet

### Command to Run:
spark-submit --class "purandare_himanshu_clustering" --master local[*] purandare_himanshu_clustering.jar "*<Path_To_Iris_data_File>*" *<k>*

args(0): *<Path_To_Iris_data_File>*
args(1): *<k>*

### Example:
spark-submit --class "purandare_himanshu_clustering" --master local[*] purandare_himanshu_clustering.jar "clustering_data.dat" 3
