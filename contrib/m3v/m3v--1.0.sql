-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION a3v" to load this file. \quit
-- we reuse pgvector's vector
-- create extension if not exists vector;
-- create vector type
CREATE TYPE vector;

CREATE FUNCTION vector_in(cstring, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_out(vector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_recv(internal, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_send(vector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE vector (
	INPUT     = vector_in,
	OUTPUT    = vector_out,
	TYPMOD_IN = vector_typmod_in,
	RECEIVE   = vector_recv,
	SEND      = vector_send,
	STORAGE   = extended
);

-- -- functions

-- CREATE FUNCTION l2_distance(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION inner_product(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION cosine_distance(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION l1_distance(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_dims(vector) RETURNS integer
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_norm(vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_add(vector, vector) RETURNS vector
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_sub(vector, vector) RETURNS vector
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_mul(vector, vector) RETURNS vector
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- -- private functions

-- CREATE FUNCTION vector_lt(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_le(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_eq(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_ne(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_ge(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_gt(vector, vector) RETURNS bool
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_cmp(vector, vector) RETURNS int4
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_l2_squared_distance(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_negative_inner_product(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_spherical_distance(vector, vector) RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_accum(double precision[], vector) RETURNS double precision[]
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_avg(double precision[]) RETURNS vector
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION vector_combine(double precision[], double precision[]) RETURNS double precision[]
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- CREATE FUNCTION m3v_am_handler(internal)
-- RETURNS table_am_handler
-- AS 'MODULE_PATHNAME'
-- LANGUAGE C;

-- -- Access method
-- CREATE ACCESS METHOD m3v_am TYPE TABLE HANDLER m3v_am_handler;
-- COMMENT ON ACCESS METHOD m3v_am IS 'template table AM eating all data';

-- CREATE FUNCTION Print() RETURNS float8
-- 	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


CREATE FUNCTION l2_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <-> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

-- bind m3v index
CREATE FUNCTION a3vhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD a3v TYPE INDEX HANDLER a3vhandler;

COMMENT ON ACCESS METHOD a3v IS 'a3v index access method';

-- CREATE OPERATOR CLASS vector_cosine_ops
-- 	FOR TYPE vector USING m3v AS
-- 	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
-- 	FUNCTION 1 cosine_distance(vector, vector),
-- 	FUNCTION 2 vector_norm(vector),
-- 	FUNCTION 3 vector_spherical_distance(vector, vector),
-- 	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING a3v AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	-- FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 1 l2_distance(vector, vector);

-- ALTER OPERATOR FAMILY vector_l2_ops USING m3v ADD
--   -- cross-type comparisons int8 vs int2
--   OPERATOR 1 <> (vector, vector),
--   FUNCTION 1 l2_distance(vector, vector);