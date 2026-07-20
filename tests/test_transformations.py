"""
Tests for transformations module.
"""
import pytest
from pyspark.sql.functions import col

from src.transformations import (
    add_trip_duration,
    add_datetime_features,
    add_is_weekend,
    add_time_of_day,
    add_speed,
    add_tip_percentage,
    add_payment_type_name,
    add_airport_flags,
    add_all_features,
)


class TestTripDuration:
    """tests for trip_duration_minutes."""

    def test_adds_duration_column(self, sample_taxi_data):
        result = add_trip_duration(sample_taxi_data)
        assert "trip_duration_minutes" in result.columns

    def test_duration_correct_value(self, sample_taxi_data):
        """First course is 20 minutes (9:00 -> 9:20)."""
        result = add_trip_duration(sample_taxi_data)
        first_row = result.filter(col("VendorID") == 1) \
            .filter(col("passenger_count") == 2) \
            .collect()[0]

        assert first_row["trip_duration_minutes"] == 20.0

    def test_duration_never_negative(self, sample_taxi_data):
        """Sanity check - duration should be >= 0."""
        result = add_trip_duration(sample_taxi_data)
        negative_count = result.filter(col("trip_duration_minutes") < 0).count()
        assert negative_count == 0


class TestDatetimeFeatures:
    """tests for datetime features."""

    def test_adds_all_datetime_columns(self, sample_taxi_data):
        result = add_datetime_features(sample_taxi_data)
        expected = ["pickup_year", "pickup_month", "pickup_day",
                    "pickup_hour", "pickup_day_of_week", "pickup_day_name"]
        for col_name in expected:
            assert col_name in result.columns

    def test_pickup_hour_extracted_correctly(self, sample_taxi_data):
        """first course is in 9 AM."""
        result = add_datetime_features(sample_taxi_data)
        first_row = result.filter(col("VendorID") == 1) \
            .filter(col("passenger_count") == 2) \
            .collect()[0]

        assert first_row["pickup_hour"] == 9

    def test_year_is_2024(self, sample_taxi_data):
        """All our examples are from 2024."""
        result = add_datetime_features(sample_taxi_data)
        wrong_year_count = result.filter(col("pickup_year") != 2024).count()
        assert wrong_year_count == 0


class TestIsWeekend:
    """Tests for weekend detection."""

    def test_adds_is_weekend_column(self, sample_taxi_data):
        df = add_datetime_features(sample_taxi_data)
        result = add_is_weekend(df)
        assert "is_weekend" in result.columns

    def test_saturday_is_weekend(self, sample_taxi_data):
        """2024-01-06 is Saturday."""
        df = add_datetime_features(sample_taxi_data)
        result = add_is_weekend(df)

        saturday_row = result.filter(col("VendorID") == 2).collect()[0]
        assert saturday_row["is_weekend"] == True

    def test_monday_not_weekend(self, sample_taxi_data):
        """2024-01-08 is Monday."""
        df = add_datetime_features(sample_taxi_data)
        result = add_is_weekend(df)

        monday_row = result.filter(col("VendorID") == 1) \
            .filter(col("passenger_count") == 2).collect()[0]
        assert monday_row["is_weekend"] == False


class TestTimeOfDay:
    """tests for time_of_day bucketing."""

    def test_morning_bucket(self, sample_taxi_data):
        """9 AM -> Morning."""
        df = add_datetime_features(sample_taxi_data)
        result = add_time_of_day(df)

        morning_row = result.filter(col("pickup_hour") == 9).collect()[0]
        assert morning_row["time_of_day"] == "Morning"

    def test_afternoon_bucket(self, sample_taxi_data):
        """14:30 -> Afternoon."""
        df = add_datetime_features(sample_taxi_data)
        result = add_time_of_day(df)

        afternoon_row = result.filter(col("pickup_hour") == 14).collect()[0]
        assert afternoon_row["time_of_day"] == "Afternoon"

    def test_evening_bucket(self, sample_taxi_data):
        """20:00 -> Evening."""
        df = add_datetime_features(sample_taxi_data)
        result = add_time_of_day(df)

        evening_row = result.filter(col("pickup_hour") == 20).collect()[0]
        assert evening_row["time_of_day"] == "Evening"


class TestSpeed:
    """tests for speed calculation."""

    def test_speed_calculation(self, sample_taxi_data):
        """5 miles for 20 minutes = 15 mph."""
        df = add_trip_duration(sample_taxi_data)
        result = add_speed(df)

        first_row = result.filter(col("VendorID") == 1) \
            .filter(col("passenger_count") == 2).collect()[0]

        # 5 miles / (20/60) hours = 15 mph
        assert first_row["speed_mph"] == 15.0

    def test_speed_never_negative(self, sample_taxi_data):
        df = add_trip_duration(sample_taxi_data)
        result = add_speed(df)
        negative = result.filter(col("speed_mph") < 0).count()
        assert negative == 0


class TestTipPercentage:
    """tests for tip percentage."""

    def test_tip_percentage_calculation(self, sample_taxi_data):
        """first course: tip=3, fare=15 -> 20%."""
        result = add_tip_percentage(sample_taxi_data)

        first_row = result.filter(col("VendorID") == 1) \
            .filter(col("passenger_count") == 2).collect()[0]

        assert first_row["tip_percentage"] == 20.0

    def test_zero_tip_zero_percentage(self, sample_taxi_data):
        """second course: tip=0, fare=10 -> 0%."""
        result = add_tip_percentage(sample_taxi_data)

        second_row = result.filter(col("VendorID") == 2).collect()[0]
        assert second_row["tip_percentage"] == 0.0


class TestPaymentTypeName:
    """tests for payment_type -> name mapping."""

    def test_credit_card_mapping(self, sample_taxi_data):
        result = add_payment_type_name(sample_taxi_data)
        row = result.filter(col("payment_type") == 1).collect()[0]
        assert row["payment_type_name"] == "Credit card"

    def test_cash_mapping(self, sample_taxi_data):
        result = add_payment_type_name(sample_taxi_data)
        row = result.filter(col("payment_type") == 2).collect()[0]
        assert row["payment_type_name"] == "Cash"


class TestAirportFlags:
    """tests for airport detection."""

    def test_jfk_pickup_detected(self, sample_taxi_data):
        """PULocationID 132 = JFK."""
        result = add_airport_flags(sample_taxi_data)

        jfk_row = result.filter(col("PULocationID") == 132).collect()[0]
        assert jfk_row["is_pickup_airport"] == True
        assert jfk_row["is_airport_trip"] == True

    def test_non_airport_zone(self, sample_taxi_data):
        """Zone 100 is not airport."""
        result = add_airport_flags(sample_taxi_data)

        row = result.filter(col("PULocationID") == 100).collect()[0]
        assert row["is_pickup_airport"] == False


class TestFullPipeline:
    """Integration test - all pipeline."""

    def test_add_all_features_no_row_loss(self, sample_taxi_data):
        """Feature engineering SHOULD NOT LOSE ROWS."""
        initial_count = sample_taxi_data.count()
        result = add_all_features(sample_taxi_data)
        assert result.count() == initial_count

    def test_add_all_features_adds_columns(self, sample_taxi_data):
        """Should add atleast 15 new columns."""
        initial_cols = len(sample_taxi_data.columns)
        result = add_all_features(sample_taxi_data)
        assert len(result.columns) >= initial_cols + 15