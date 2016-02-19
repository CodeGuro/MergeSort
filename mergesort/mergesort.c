#include <stdio.h>
#include <stdlib.h>

int * mergesort( int *arr, int n )
{
	if( n < 2 )
		return arr;

	if( n == 2 )
	{
		if( arr[ 0 ] > arr[ 1 ] )
		{
			int tmp = arr[ 0 ];
			arr[ 0 ] = arr[ 1 ];
			arr[ 1 ] = tmp;
		}
		return arr;
	}

	int k = n / 2;
	int is_odd = n & 0x01;

	int *buff = malloc( n * sizeof( int ) );

	int *fhalf = mergesort( arr, k + is_odd );
	int *shalf = mergesort( arr + k + is_odd, k );
	
	int *fhalf_end = shalf;
	int *shalf_end = fhalf + n;
	int *ptr = buff;
	do
	{
		while( fhalf < fhalf_end && shalf < shalf_end )
		{
			if( *fhalf < *shalf )
				*ptr++ = *fhalf++;
			else
				*ptr++ = *shalf++;
		}

		if( fhalf == fhalf_end )
			while( shalf < shalf_end )
				*ptr++ = *shalf++;
		else if( shalf == shalf_end )
			while( fhalf < fhalf_end )
				*ptr++ = *fhalf++;

	} while( fhalf < fhalf_end && shalf < shalf_end );

	for( int it = 0; it < n; ++it )
		arr[ it ] = buff[ it ];

	free( buff );

	return arr;
}

int main( int argc, char **argv )
{
	FILE *hfile = fopen( argv[ 1 ], "rb" );

	if( hfile == NULL )
		return printf( "no such file %s", "%s" );

	char buffer[ 1024 ] = { 0 };

	fgets( buffer, sizeof( buffer ), hfile );
	int arrsize = atoi( buffer );
	int *buff = malloc( arrsize * sizeof( int ) );
	for( int it = 0; it < arrsize; ++it )
	{
		char mini_buff[ 128 ] = { 0 };
		char c;
		while( isspace( (c = fgetc( hfile )) ) );
		for( int it2 = 0; it2 < sizeof( mini_buff ) && !isspace( c ); c = fgetc( hfile ) )
			mini_buff[ it2++ ] = c;

		buff[ it ] = atoi( mini_buff );
	}

	fclose( hfile );

	mergesort( buff, arrsize );

	for( int it = 0; it < arrsize; ++it )
		printf( "%d\n", buff[ it ] );

	free( buff );

	return 0;
}