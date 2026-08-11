/*
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Copyright (C) 2026 Cyril "Parad0x141" Bouvier
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include <cstdint>
#include <string>


/// <summary>
///  Does this need some explantions ?
/// </summary>
/// <param name="v"></param>
/// <returns></returns>
static size_t RoundUpPowerOf2(size_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    return v + 1;
}

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
