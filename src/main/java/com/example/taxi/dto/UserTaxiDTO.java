package com.example.taxi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserTaxiDTO {

    private boolean is_active;
    private int taxi_id;
    private int user_id;
}
