package com.example.taxi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserTaxiPositionDTO {

    private java.lang.Integer distance;
    private java.lang.String user_lat_id;
    private java.lang.String user_long_id;
    private java.lang.String taxi_lat_id;
    private java.lang.String taxi_long_id;
    private java.lang.Integer taxi_id;
    private java.lang.Integer user_id;
}
