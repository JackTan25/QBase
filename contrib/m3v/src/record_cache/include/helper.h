#pragma once

template <typename T>
constexpr T MinValue(T a, T b) {
	return a < b ? a : b;
}