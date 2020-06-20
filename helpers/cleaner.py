





# Steps :
""":arg
1 - Clean demographics dataset, filling null values withn 0
2 - Clean airports dataset filtering only US airports and discarting anything else that is not an airport. Extract iso regions and cast as float elevation feet.


"""

class Cleaner():
    def __init__(self,spark):
        self.spark = spark

    @staticmethod
    def cleaning_demographics_data(dataFrame):
        # filling null with 0
        dataFrame = dataFrame.na.fill({
            'Median Age':0,'Male Population':0,'Female Population':0,'Total Population':0,'Number of Veterans':0,
            "Foreign-born":0,'Average Household Size':0,'Count':0,
        })
        dataFrame =dataFrame.dropDuplicates()
        return dataFrame

    @staticmethod
    def cleanning_airports_data(dataFrame):
        dataFrame = dataFrame.where()