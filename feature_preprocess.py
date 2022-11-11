from dateutil import parser
from pyspark import SparkContext


class FeaturePreprocess():
    
    def __init__(self):
        self.data = {}
    
    def orderIdColumn(self):
        try:
            self.data["OrderId"] = int(rdd.collect()[0])
        except Exception as error:
            self.data["OrderId"] = None
    
    def userNameColumn(self):
        print(rdd.collect())
        try:
            self.data["UserName"] = str(rdd.collect()[1])
        except Exception as error:
            self.data["UserName"] = None
    
    def productNameColumn(self):
        try:
            self.data["ProductName"] = str(rdd.collect()[2])
        except Exception as error:
            self.data["ProductName"] = None
    
    def priceColumn(self):
        try:
            self.data["Price"] = float(rdd.collect()[3])
            if (((self.data["Price"])>99) | ((self.data["Price"])<0)):
                self.data["Price"] = None
        except Exception as error:
            self.data["Price"] = None
    
    def adressColumn(self):
        try:
            self.data["Adress"] = str(rdd.collect()[4])
            self.data["StreetName"] = str(rdd.collect()[4]).split(maxsplit=1)[1]
            if len(str(self.data["StreetName"]).split()) < 2:
                self.data["StreetName"] = None
        except Exception as error:
            self.data["Adress"] = None
            self.data["StreetName"] = None
    
    def latitudeColumn(self):
        try:
            self.data["Latitude"] = float(rdd.collect()[5])
        except Exception as error:
            self.data["Latitude"] = None
    
    def longitudeColumn(self):
        try:
            self.data["Longitude"] = float(rdd.collect()[6])
        except Exception as error:
            self.data["Longitude"] = None
                 
    def timeStampColumn(self):
        """Check Datetime format
        """
        try:
            self.data["TimeStamp"] = parser.isoparse(rdd.collect()[7])
        except Exception as error:
            self.data["TimeStamp"] = None
    
    def structureData(self, sc, msg):
        """Structure data using spark parallilize

        Args:
            data (Dict): data preprocessed
        """
        # Create RDD to parellize the tasks
        global rdd
        rdd=sc.parallelize(msg)
        # Peform validations and transformations
        self.orderIdColumn(), self.userNameColumn(), self.productNameColumn(), self.priceColumn()
        self.adressColumn(), self.latitudeColumn(), self.longitudeColumn(), self.timeStampColumn()
        return self.data