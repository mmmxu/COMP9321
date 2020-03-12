import ast
import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os

studentid = os.path.basename(sys.modules[__name__].__file__)


#################################################
# Your personal methods can be here ...
#################################################


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))
    if other is not None:
        print(question, other)
    if output_df is not None:
        print(output_df.head(5).to_string())


def question_1(movies, credits):
    """
    :param movies: the path for the movie.csv file
    :param credits: the path for the credits.csv file
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    movies_df = pd.read_csv(movies)
    credits_df = pd.read_csv(credits)
    df1 = pd.merge(left=movies_df, right=credits_df,on=None, left_on='id', right_on='id')
    
    #################################################

    log("QUESTION 1", output_df=df1, other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df2
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df2 = df1[[ 'id', 'title', 'popularity', 'cast', 'crew', 'budget', 'genres', 'original_language', 'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'vote_average', 'vote_count']]
    #################################################

    log("QUESTION 2", output_df=df2, other=(len(df2.columns), sorted(df2.columns)))
    return df2


def question_3(df2):
    """
    :param df2: the dataframe created in question 2
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df3 = df2.set_index('id')
    #################################################

    log("QUESTION 3", output_df=df3, other=df3.index.name)
    return df3


def question_4(df3):
    """
    :param df3: the dataframe created in question 3
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df4 = df3[df3.budget!=0]
    #################################################

    log("QUESTION 4", output_df=df4, other=(df4['budget'].min(), df4['budget'].max(), df4['budget'].mean()))
    return df4


def question_5(df4):
    """
    :param df4: the dataframe created in question 4
    :return: df5
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df5 = df4.copy()
    df5['success_impact'] = (df5['revenue'] - df5['budget'])/df5['budget']
    #################################################

    log("QUESTION 5", output_df=df5,
        other=(df5['success_impact'].min(), df5['success_impact'].max(), df5['success_impact'].mean()))
    return df5


def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df6 = df5.copy()
    df6 = df6.astype({'popularity':'float'})
    df6['popularity']= (df6['popularity']-df6['popularity'].min())/(df6['popularity'].max()-df6['popularity'].min())*100
    #################################################

    log("QUESTION 6", output_df=df6, other=(df6['popularity'].min(), df6['popularity'].max(), df6['popularity'].mean()))
    return df6


def question_7(df6):
    """
    :param df6: the dataframe created in question 6
    :return: df7
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df7 = df6.copy()
    df7 = df7.astype({'popularity':'int16'})
    #################################################

    log("QUESTION 7", output_df=df7, other=df7['popularity'].dtype)
    return df7


def question_8(df7):
    """
    :param df7: the dataframe created in question 7
    :return: df8
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    df8 = df7.copy()
    for i, row in df8.iterrows():
        cast_data = row['cast']
        cast_lst = ast.literal_eval(cast_data)
        total_cast_lst = []
        for j in cast_lst:
            # convert to dict
            single_cast_dict = ast.literal_eval(str(j))
            total_cast_lst.append(single_cast_dict['character'])
        # sort by character
        total_cast_lst = sorted(total_cast_lst)
        total_cast_str = ','.join([str(elem) for elem in total_cast_lst]) 
        df8['cast'][i] = total_cast_str
    #################################################

    log("QUESTION 8", output_df=df8, other=df8["cast"].head(10).values)
    return df8


def question_9(df8):
    """
    :param df9: the dataframe created in question 8
    :return: movies
            Data Type: List of strings (movie titles)
            Please read the assignment specs to know how to create the output
    """

    #################################################
    # Your code goes here ...
    # QUESTION do we need to keep (uncredited)?
    df9 = df8[['title', 'cast']]
    cast_count_dict = {}
    for idx, row in df9.iterrows():
        cast_lst = (row['cast']).split(",")
        # remove dumplicate
        cast_lst = list(dict.fromkeys(cast_lst))
        cast_num = len(cast_lst)
        cast_count_dict[idx] = cast_num

    top10_film_id = [key for key in sorted(cast_count_dict, key=cast_count_dict.get, reverse=True)[:10]]
    top_10_film_lst = []

    for f_id in top10_film_id:
        top_10_film_lst.append(df9['title'][f_id])
    #################################################

    log("QUESTION 9", output_df=None, other=movies)
    return movies


def question_10(df8):
    """
    :param df8: the dataframe created in question 8
    :return: df10
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    # QUESTION! can we convert to datetime type
    df10 = df8.copy()
    df10['release_date']=pd.to_datetime(df10.release_date)
    df10 = df10.sort_values(by='release_date', ascending=False)
    #################################################

    log("QUESTION 10", output_df=df10, other=df10["release_date"].head(5).to_string().replace("\n", " "))
    return df10


def question_11(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    # % display? text overlapping?
    df11=df10.copy()
    for i, row in df11.iterrows():
        genre_data = row['genres']
        genre_lst = ast.literal_eval(genre_data)
        total_genre_lst = []
        for j in genre_lst:
            # convert to dict
            single_genre_dict = ast.literal_eval(str(j))
            total_genre_lst.append(single_genre_dict['name'])
        # sort by character
        total_genre_lst = sorted(total_genre_lst)
        df11['genres'][i] = total_genre_lst

    ## calc
    film_num = 0
    genre_dict = {}
    for i, row in df11.iterrows():
        genre_lst = row['genres']
        film_num += len(genre_lst)
        for g in genre_lst:
            if not g in genre_dict:
                genre_dict[g] = 1
            else:
                genre_dict[g] += 1
    
    # combine least 4 values
    least_4_vals = 0
    for _ in range(4):
        min_key = min(genre_dict.keys(), key= lambda x:genre_dict[x])
        least_4_vals += genre_dict[min_key]
        del genre_dict[min_key]
    genre_dict['Others'] = least_4_vals

    # convert to dataframe and plot out        
    pd_out = pd.DataFrame(genre_dict.items(), columns=['genre','count'])
    plt.pie(pd_out['count'], labels=pd_out['genre'], startangle=90,autopct='%1.1f%%',)
    #################################################

    plt.savefig("{}-Q11.png".format(studentid))


def question_12(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    df12=df10.copy()
    for i, row in df12.iterrows():
        p_cty_data = row['production_countries']
        p_cty_lst = ast.literal_eval(p_cty_data)
        total_cty_lst = []
        for j in p_cty_lst:
            # convert to dict
            single_cty_dict = ast.literal_eval(str(j))
            total_cty_lst.append(single_cty_dict['name'])
        # sort by character
        total_cty_lst = sorted(total_cty_lst)
        df12['production_countries'][i] = total_cty_lst


    ## calc
    cty_num = 0
    cty_dict = {}
    for i, row in df12.iterrows():
        cty_lst = row['production_countries']
        cty_num += len(cty_lst)
        for g in cty_lst:
            if not g in cty_dict:
                cty_dict[g] = 1
            else:
                cty_dict[g] += 1
    # sort by value
    cty_dict_sorted = {k: v for k, v in sorted(cty_dict.items(), key=lambda i: i[1])}

    # # convert to dataframe and plot out        
    pd_out = pd.DataFrame(cty_dict_sorted.items(), columns=['cty','count'])
    pd_out.plot.bar(x='cty', y='count')
    #################################################

    plt.savefig("{}-Q12.png".format(studentid))


def question_13(df10):
    """
    :param df10: the dataframe created in question 10
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################

    plt.savefig("{}-Q13.png".format(studentid))


if __name__ == "__main__":
    df1 = question_1("movies.csv", "credits.csv")
    df2 = question_2(df1)
    df3 = question_3(df2)
    df4 = question_4(df3)
    df5 = question_5(df4)
    df6 = question_6(df5)
    df7 = question_7(df6)
    df8 = question_8(df7)
    movies = question_9(df8)
    df10 = question_10(df8)
    question_11(df10)
    question_12(df10)
    question_13(df10)
