from utils import helpers
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window

class Accident:
    def __init__(self,spark,config):
        self.config=config
        self.df_primary_person=helpers.read_csv(spark,config['INPUTPATH']['PRIMARY_PERSON'])
        self.df_units=helpers.read_csv(spark,config['INPUTPATH']['UNITS'])
        self.df_damages=helpers.read_csv(spark,config['INPUTPATH']['DAMAGES'])
        self.df_charges=helpers.read_csv(spark,config['INPUTPATH']['CHARGES'])

    def count_male_accidents(self):
        """
        Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        :return: dataframe distinct crash count
        """
        # analytics1=self.df_primary_person\
        #             .select(self.df_primary_person.CRASH_ID,self.df_primary_person.PRSN_INJRY_SEV_ID,self.df_primary_person.PRSN_GNDR_ID)\
        #             .filter((self.df_primary_person['PRSN_INJRY_SEV_ID']=='KILLED' ) & (self.df_primary_person['PRSN_GNDR_ID']=='MALE'))
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "MALE")        
        # helpers.write_output(df, self.config['OUTPUTPATH']["TASK1"])
        return df.distinct().count()

    def count_2_wheeler_accidents(self):
        """
        Analysis 2: How many two wheelers are booked for crashes?
        :return: dataframe distinct crash count
        """
        analytics2=self.df_units.filter(self.df_units['VEH_BODY_STYL_ID']=='MOTORCYCLE' ).select(self.df_units.CRASH_ID)
        return analytics2.distinct().count()

    def get_state_with_highest_female_accident(self):
        """
        Analysis 3: Which state has highest number of accidents in which females are involved?
        :return: state name with highest female accidents
        """
        analytics3 = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "FEMALE").groupby("DRVR_LIC_STATE_ID").count().withColumnRenamed("count","accident_count")
        analytics3=analytics3.sort(analytics3.accident_count.desc())
        return analytics3.first().DRVR_LIC_STATE_ID

    def get_top_5_15_vehicle_make_contributing_to_injuries(self):
        """
        Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :return: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        analytics4 = self.df_units\
            .select(self.df_units.CRASH_ID,self.df_units.VEH_MAKE_ID,self.df_units.TOT_INJRY_CNT,self.df_units.DEATH_CNT)\
            .withColumn('injury_and_death', self.df_units['TOT_INJRY_CNT'] + self.df_units['DEATH_CNT'])\
            .groupBy("VEH_MAKE_ID").sum("injury_and_death") \
            .withColumnRenamed("sum(injury_and_death)", "total_injury_and_death")
        analytics4=analytics4.sort(analytics4.total_injury_and_death.desc())   
        analytics4=analytics4.limit(15).subtract(analytics4.limit(5)).select("VEH_MAKE_ID")
        return [ x[0] for x in analytics4.collect()]
    

    def get_top_person_ethnicity_for_each_body_vehicle_style(self):
        """
        Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        :return: None
        """
        window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        analytics5 = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            filter(~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row", row_number().over(window)).filter(col("row") == 1).drop("row", "count")

        helpers.write_output(analytics5, self.config['OUTPUTPATH']["TASK5"])
        analytics5.show(truncate=False)

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self):
        """
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code
        :return: List of Zip Codes
        """
        analytics6 = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        helpers.write_output(analytics6, self.config['OUTPUTPATH']["TASK6"])
        return [row[0] for row in analytics6.collect()]

    def get_crash_ids_with_no_damage(self):
        """
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        :return: List of crash ids
        """
        analytics7 = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner'). \
            filter(
            (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
        ). \
            filter(self.df_damages.DAMAGED_PROPERTY == "NONE"). \
            filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        helpers.write_output(analytics7, self.config['OUTPUTPATH']["TASK7"])

        return [row[0] for row in analytics7.collect()]
    
    def get_top_5_vehicle_brand(self):
        """
        Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        :return List of Vehicle brands
        """
        top_25_state_list = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        analytics8 = self.df_charges.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(self.df_units, on=['CRASH_ID'], how='inner'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        helpers.write_output(analytics8, self.config['OUTPUTPATH']["TASK8"])

        return [row[0] for row in analytics8.collect()]
    
    def run(self):
        # 1. Find the number of crashes (accidents) in which number of persons killed are male?
        print("1. Result:", self.count_male_accidents())

        # 2. How many two-wheelers are booked for crashes?
        print("2. Result:", self.count_2_wheeler_accidents())

        # 3. Which state has the highest number of accidents in which females are involved?
        print("3. Result:", self.get_state_with_highest_female_accident())

        # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        print("4. Result:", self.get_top_5_15_vehicle_make_contributing_to_injuries())

        # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        print("5. Result:")
        self.get_top_person_ethnicity_for_each_body_vehicle_style()

        # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the
        # contributing factor to a crash (Use Driver Zip Code)
        print("6. Result:", self.get_top_5_zip_codes_with_alcohols_as_cf_for_crash())

        # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        # and car avails Insurance
        print("7. Result:", self.get_crash_ids_with_no_damage())

        # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        # offences (to be deduced from the data)
        print("8. Result:", self.get_top_5_vehicle_brand())