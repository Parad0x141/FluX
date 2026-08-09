#pragma once
#include <cstdint>
#include <string>


/// Format an integer with thousands separators (e.g. 3143760 -> "3,143,760").
/// Deliberately not locale-based: std::locale("") depends on locale data
/// being installed/configured on the target machine (not guaranteed on a
/// fresh Windows box), so this just does it manually same result,
/// zero external dependency.
static std::string FormatWithCommas(uint64_t value)
{
    std::string s = std::to_string(value);
    int insert_pos = static_cast<int>(s.length()) - 3;
    while (insert_pos > 0)
    {
        s.insert(insert_pos, ",");
        insert_pos -= 3;
    }
    return s;
}
