#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <mpi.h>

// sorting algorithm of complexity O( nlogn )
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

	while( fhalf < fhalf_end && shalf < shalf_end )
	{
		if( *fhalf < *shalf )
			*ptr++ = *fhalf++;
		else
			*ptr++ = *shalf++;
	}

	while( shalf < shalf_end )
		*ptr++ = *shalf++;
	while( fhalf < fhalf_end )
		*ptr++ = *fhalf++;

	for( int it = 0; it < n; ++it )
		arr[ it ] = buff[ it ];

	free( buff );

	return arr;
}

// sorting algorithm of complexity O( n )
// this function assumes the both buffers of are sorted
void quick_merge( int *out, int *b1, int *b2, int n )
{
	int *b1_end = b1 + n;
	int *b2_end = b2 + n;

	while( b1 < b1_end && b2 < b2_end )
	{
		if( *b1 < *b2 )
			*out++ = *b1++;
		else
			*out++ = *b2++;
	}

	while( b1 < b1_end )
		*out++ = *b1++;
	while( b2 < b2_end )
		*out++ = *b2++;
}

// this function writes the output to the specified filename
void write_outfile( char const *fname, int *buff, int n )
{
	FILE *hfile = fopen( fname, "w" );

	fprintf( hfile, "Sorted array of %i elements:", n );

	for( int it = 0; it < n; ++it )
	{
		const char *delim = it % 10 ? " " : "\n";
		fprintf( hfile, "%s%i", delim, buff[ it ] );
	}

	fclose( hfile );
}

int main( int argc, char **argv )
{
	MPI_Init( &argc, &argv );

	int rank;
	int comm_size;
	MPI_Comm_rank( MPI_COMM_WORLD, &rank );
	MPI_Comm_size( MPI_COMM_WORLD, &comm_size );

	printf( "this rank is %i and the size is %i\n", rank, comm_size );
	int *buff = NULL;
	int arrsize = -1;
	int *rec_buff = NULL;
	int rec_buff_size = -1;

	if( rank == 0 ) // parent
	{
		FILE *hfile = fopen( argv[ 1 ], "rb" );

		if( hfile == NULL )
			return printf( "no such file %s", "%s" );

		char buffer[ 1024 ] = { 0 };

		fgets( buffer, sizeof( buffer ), hfile );
		arrsize = atoi( buffer );
		rec_buff_size = ( arrsize / comm_size ) + ( ( arrsize % comm_size ) ? 1 : 0 );

		buff = malloc( rec_buff_size * comm_size * sizeof( int ) );
		// map the buffer with the integers
		for( int it = 0; it < arrsize; ++it )
		{
			char mini_buff[ 128 ] = { 0 };
			char c;
			while( isspace( ( c = fgetc( hfile ) ) ) );
			for( int it2 = 0; it2 < sizeof( mini_buff ) && !isspace( c ); c = fgetc( hfile ) )
				mini_buff[ it2++ ] = c;

			buff[ it ] = atoi( mini_buff );
		}

		// fill the rest with max_int (to be removed later)
		for( int it = arrsize; it < rec_buff_size * comm_size; ++it )
			buff[ it ] = INT_MAX;
		
		rec_buff = malloc( rec_buff_size * sizeof( int ) );
		MPI_Bcast( &rec_buff_size, 1, MPI_INT, 0, MPI_COMM_WORLD );
		MPI_Scatter( buff, rec_buff_size, MPI_INT, rec_buff, rec_buff_size, MPI_INT, 0, MPI_COMM_WORLD );
		printf( "parent broadcast sent\n" );
		printf( "child rank %i recieved the buffer size. It is %i\n", rank, rec_buff_size );

		// clean up
		free( buff );
		fclose( hfile );
	}
	else // child
	{
		printf( "Entering child rank %i\n", rank );
		MPI_Bcast( &rec_buff_size, 1, MPI_INT, 0, MPI_COMM_WORLD );
		printf( "child rank %i recieved the buffer size. It is %i\n", rank, rec_buff_size );
		rec_buff = malloc( rec_buff_size * sizeof( int ) );
		MPI_Scatter( NULL, 0, MPI_DATATYPE_NULL, rec_buff, rec_buff_size, MPI_INT, 0, MPI_COMM_WORLD );
		printf( "child rank %i recieved the data\n", rank );
	}

	mergesort( rec_buff, rec_buff_size );

	for( int step = 1; step < comm_size; step *= 2 )
	{
		if( rank % (2 * step) )
		{
			MPI_Send( rec_buff, rec_buff_size, MPI_INT, rank - step, 0, MPI_COMM_WORLD ); // send data for merging
			break; // we are now finished for this proc
		}
		else
		{
			if( rank + step < comm_size )
			{
				printf( "rank %i is merging from rank %i\n", rank, rank + step );
				int *second_buff = malloc( rec_buff_size * sizeof( int ) );
				int *new_rec_buff = malloc( rec_buff_size * 2 * sizeof( int ) );
				MPI_Recv( second_buff, rec_buff_size, MPI_INT, rank + step, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE );
				quick_merge( new_rec_buff, rec_buff, second_buff, rec_buff_size );
				free( second_buff );
				free( rec_buff );
				rec_buff = new_rec_buff;
				rec_buff_size *= 2;
			}
		}
	}

	if( rank == 0 )
		write_outfile( argv[ 2 ], rec_buff, arrsize );

	free( rec_buff );

	MPI_Finalize();

	return 0;
}