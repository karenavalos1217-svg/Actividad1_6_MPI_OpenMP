
#include <iostream>
#include <string>
#include <cstring>
#include <limits>
#include <fstream>
#include <atomic>
#include <algorithm>
#include <iomanip>
#include <mpi.h>
#include <omp.h>

using namespace std;

const int TAG_RESULTADO = 100;
const int TAG_CANCELAR = 101;
const int TAG_FINALIZAR = 102;

const unsigned long long TAM_BLOQUE = 4096ULL;

struct ResultadoBusqueda {
    unsigned long long indice = 0;
    unsigned long long revisadas = 0;
    int hilo = -1;
    int encontrado = 0;
    int cancelado = 0;
    double tiempo = 0;
};

bool validarClave(const string& clave) {
    if (clave.empty() || clave.length() > 10) {
        return false;
    }

    for (char c : clave) {
        if (!((c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9'))) {
            return false;
        }
    }

    return true;
}

unsigned long long calcularCombinaciones(int longitud) {
    unsigned long long total = 1;

    for (int i = 0; i < longitud; i++) {
        total *= 36ULL;
    }

    return total;
}

void convertirIndiceATexto(
    unsigned long long indice,
    char* candidato,
    int longitud
) {
    const char* alfabeto =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    for (int i = longitud - 1; i >= 0; i--) {
        candidato[i] = alfabeto[indice % 36ULL];
        indice /= 36ULL;
    }

    candidato[longitud] = '\0';
}

void mostrarIntegrantes() {
    cout << "Avalos Oliva Karen Lizeth\n";
    cout << "Chavez Torres Oliver Daniel\n";
    cout << "Ramirez Arvizu Fernando David\n";
}

void mostrarMenu() {
    cout << "\nACTIVIDAD 1.6 - FUERZA BRUTA MPI + OPENMP\n";
    cout << "1. Ingresar clave personalizada\n";
    cout << "2. Ejecutar busqueda secuencial\n";
    cout << "3. Ejecutar busqueda paralela distribuida\n";
    cout << "4. Mostrar comparativa de tiempos\n";
    cout << "5. Salir\n";
    cout << "Opcion: ";
}

string obtenerNombreEquipo() {
    char nombre[MPI_MAX_PROCESSOR_NAME];
    int longitud = 0;

    MPI_Get_processor_name(nombre, &longitud);

    return string(nombre, longitud);
}

string obtenerNombreLog(int rank) {
    return "log_equipo_" + obtenerNombreEquipo() +
        "_nodo_" + to_string(rank) + ".txt";
}

void busquedaSecuencial(
    const string& clave,
    double& tiempoSecuencial
) {
    if (clave.empty()) {
        cout << "\nPrimero ingresa una clave.\n";
        return;
    }

    int longitud = static_cast<int>(clave.length());
    unsigned long long total =
        calcularCombinaciones(longitud);

    if (longitud > 5) {
        char confirmar;

        cout << "\nADVERTENCIA: esta busqueda puede "
            << "tardar mucho tiempo.\n";
        cout << "Deseas continuar? (S/N): ";
        cin >> confirmar;

        if (confirmar != 'S' && confirmar != 's') {
            cout << "Busqueda cancelada.\n";
            return;
        }
    }

    char* candidato = new char[longitud + 1];

    bool encontrado = false;
    unsigned long long indiceEncontrado = 0;
    unsigned long long revisadas = 0;

    cout << "\nINICIANDO BUSQUEDA SECUENCIAL\n";
    cout << "Clave objetivo: " << clave << "\n";
    cout << "Total de combinaciones: " << total << "\n";

    double inicioTiempo = MPI_Wtime();

    for (unsigned long long i = 0; i < total; i++) {
        convertirIndiceATexto(i, candidato, longitud);
        revisadas++;

        if (strcmp(candidato, clave.c_str()) == 0) {
            encontrado = true;
            indiceEncontrado = i;
            break;
        }
    }

    tiempoSecuencial = MPI_Wtime() - inicioTiempo;

    cout << "\nRESULTADOS DE BUSQUEDA SECUENCIAL\n";

    if (encontrado) {
        cout << "Clave encontrada: " << candidato << "\n";
        cout << "Indice encontrado: "
            << indiceEncontrado << "\n";
    }
    else {
        cout << "Clave no encontrada.\n";
    }

    cout << "Combinaciones revisadas: "
        << revisadas << "\n";
    cout << "Tiempo secuencial: "
        << tiempoSecuencial << " segundos\n";
    cout << "Procesos utilizados: 1\n";
    cout << "Hilos utilizados: 1\n";

    delete[] candidato;
}

void calcularRango(
    unsigned long long total,
    int rank,
    int procesos,
    unsigned long long& inicio,
    unsigned long long& fin
) {
    unsigned long long esclavos =
        static_cast<unsigned long long>(procesos - 1);

    unsigned long long posicion =
        static_cast<unsigned long long>(rank - 1);

    unsigned long long base = total / esclavos;
    unsigned long long sobrantes = total % esclavos;

    inicio = base * posicion +
        min(posicion, sobrantes);

    fin = inicio + base +
        (posicion < sobrantes ? 1ULL : 0ULL);
}

ResultadoBusqueda buscarEnRango(
    const string& clave,
    unsigned long long inicio,
    unsigned long long fin,
    int rank
) {
    ResultadoBusqueda resultado;

    int longitud = static_cast<int>(clave.length());
    bool registroDetallado = longitud <= 3;

    atomic<bool> encontrado(false);
    atomic<bool> cancelar(false);

    atomic<unsigned long long> siguienteBloque(inicio);

    unsigned long long indiceResultado = 0;
    int hiloResultado = -1;
    unsigned long long totalRevisadas = 0;

    ofstream log(obtenerNombreLog(rank));

    log << "ACTIVIDAD 1.6 - BUSQUEDA DISTRIBUIDA\n";
    log << "Equipo: " << obtenerNombreEquipo() << "\n";
    log << "Nodo MPI: " << rank << "\n";
    log << "Rango inicial: " << inicio << "\n";
    log << "Rango final exclusivo: " << fin << "\n";
    log << "Clave objetivo: " << clave << "\n";
    log << "Registro detallado: "
        << (registroDetallado ? "SI" : "NO")
        << "\n";

    double inicioTiempo = MPI_Wtime();

    // MPI solo se consulta desde el hilo principal.
    // Cada hilo toma bloques sin usar barreras OpenMP.
#pragma omp parallel reduction(+:totalRevisadas)
    {
        int hilo = omp_get_thread_num();
        char* candidato = new char[longitud + 1];

        if (hilo == 0) {
#pragma omp critical(log_escritura)
            {
                log << "Hilos OpenMP activos: "
                    << omp_get_num_threads() << "\n";
            }
        }

        while (!encontrado.load() && !cancelar.load()) {

            if (hilo == 0) {
                int hayMensaje = 0;
                MPI_Status estado;

                MPI_Iprobe(
                    0,
                    TAG_CANCELAR,
                    MPI_COMM_WORLD,
                    &hayMensaje,
                    &estado
                );

                if (hayMensaje) {
                    int orden = 0;

                    MPI_Recv(
                        &orden,
                        1,
                        MPI_INT,
                        0,
                        TAG_CANCELAR,
                        MPI_COMM_WORLD,
                        MPI_STATUS_IGNORE
                    );

                    cancelar.store(true);
                }
            }

            if (encontrado.load() || cancelar.load()) {
                break;
            }

            unsigned long long bloque =
                siguienteBloque.fetch_add(TAM_BLOQUE);

            if (bloque >= fin) {
                break;
            }

            unsigned long long finBloque =
                min(bloque + TAM_BLOQUE, fin);

            for (unsigned long long i = bloque;
                i < finBloque;
                i++) {

                if (encontrado.load() || cancelar.load()) {
                    break;
                }

                convertirIndiceATexto(
                    i,
                    candidato,
                    longitud
                );

                totalRevisadas++;

                if (registroDetallado) {
#pragma omp critical(log_escritura)
                    {
                        log << "Nodo " << rank
                            << " | Hilo " << hilo
                            << " | Indice " << i
                            << " | Candidato "
                            << candidato << "\n";
                    }
                }

                if (strcmp(candidato, clave.c_str()) == 0) {
#pragma omp critical(resultado_escritura)
                    {
                        if (!encontrado.load()) {
                            indiceResultado = i;
                            hiloResultado = hilo;
                            encontrado.store(true);
                        }
                    }

                break;
                }
            }
        }

        delete[] candidato;
    }

    resultado.tiempo = MPI_Wtime() - inicioTiempo;
    resultado.indice = indiceResultado;
    resultado.revisadas = totalRevisadas;
    resultado.hilo = hiloResultado;
    resultado.encontrado =
        encontrado.load() ? 1 : 0;
    resultado.cancelado =
        cancelar.load() ? 1 : 0;

    log << "\nRESULTADOS DEL NODO\n";
    log << "Combinaciones revisadas: "
        << resultado.revisadas << "\n";
    log << "Tiempo: "
        << resultado.tiempo << " segundos\n";
    log << "Clave encontrada: "
        << (resultado.encontrado ? "SI" : "NO")
        << "\n";
    log << "Hilo ganador: "
        << resultado.hilo << "\n";
    log << "Cancelado por el maestro: "
        << (resultado.cancelado ? "SI" : "NO")
        << "\n";

    log.close();

    return resultado;
}

void busquedaDistribuida(
    const string& claveMaestro,
    int rank,
    int procesos,
    double& tiempoParalelo
) {
    int longitud = 0;

    if (rank == 0) {
        longitud =
            static_cast<int>(claveMaestro.length());
    }

    MPI_Bcast(
        &longitud,
        1,
        MPI_INT,
        0,
        MPI_COMM_WORLD
    );

    if (longitud == 0) {
        if (rank == 0) {
            cout << "\nPrimero ingresa una clave.\n";
        }
        return;
    }

    if (procesos < 2) {
        if (rank == 0) {
            cout << "\nSe requieren al menos "
                << "2 procesos MPI.\n";
        }
        return;
    }

    char* bufferClave = new char[longitud + 1];

    if (rank == 0) {
        memcpy(
            bufferClave,
            claveMaestro.c_str(),
            longitud + 1
        );
    }

    MPI_Bcast(
        bufferClave,
        longitud + 1,
        MPI_CHAR,
        0,
        MPI_COMM_WORLD
    );

    string clave(bufferClave);
    delete[] bufferClave;

    unsigned long long total =
        calcularCombinaciones(longitud);

    unsigned long long inicio = 0;
    unsigned long long fin = 0;

    if (rank != 0) {
        calcularRango(
            total,
            rank,
            procesos,
            inicio,
            fin
        );
    }

    MPI_Barrier(MPI_COMM_WORLD);

    double inicioTiempo = MPI_Wtime();

    if (rank == 0) {
        cout << "\nINICIANDO BUSQUEDA DISTRIBUIDA\n";
        cout << "Clave objetivo: " << clave << "\n";
        cout << "Procesos MPI: " << procesos << "\n";
        cout << "Nodos esclavos: "
            << procesos - 1 << "\n";
        cout << "Total de combinaciones: "
            << total << "\n";

        for (int nodo = 1; nodo < procesos; nodo++) {
            unsigned long long desde = 0;
            unsigned long long hasta = 0;

            calcularRango(
                total,
                nodo,
                procesos,
                desde,
                hasta
            );

            cout << "Nodo " << nodo
                << ": [" << desde
                << ", " << hasta << ")\n";
        }

        bool encontrado = false;
        unsigned long long indiceEncontrado = 0;
        unsigned long long totalRevisadas = 0;

        int nodoEncontrado = -1;
        int hiloEncontrado = -1;
        int respuestas = 0;

        MPI_Request* solicitudes =
            new MPI_Request[procesos];

        int totalSolicitudes = 0;
        int ordenCancelacion = 1;

        ofstream logMaestro(obtenerNombreLog(0));

        logMaestro << "ACTIVIDAD 1.6 - NODO MAESTRO\n";
        logMaestro << "Equipo: "
            << obtenerNombreEquipo() << "\n";
        logMaestro << "Procesos MPI: "
            << procesos << "\n";
        logMaestro << "Clave objetivo: "
            << clave << "\n";

        while (respuestas < procesos - 1) {
            unsigned long long datos[5] = {};
            MPI_Status estado;

            MPI_Recv(
                datos,
                5,
                MPI_UNSIGNED_LONG_LONG,
                MPI_ANY_SOURCE,
                TAG_RESULTADO,
                MPI_COMM_WORLD,
                &estado
            );

            int origen = estado.MPI_SOURCE;

            respuestas++;
            totalRevisadas += datos[1];

            logMaestro << "Nodo " << origen
                << " | Revisadas: " << datos[1]
                << " | Encontrado: " << datos[3]
                << " | Cancelado: " << datos[4]
                << "\n";

            if (datos[3] == 1 && !encontrado) {
                encontrado = true;
                indiceEncontrado = datos[0];
                nodoEncontrado = origen;
                hiloEncontrado =
                    static_cast<int>(datos[2]);

                for (int nodo = 1;
                    nodo < procesos;
                    nodo++) {

                    if (nodo == nodoEncontrado) {
                        continue;
                    }

                    MPI_Isend(
                        &ordenCancelacion,
                        1,
                        MPI_INT,
                        nodo,
                        TAG_CANCELAR,
                        MPI_COMM_WORLD,
                        &solicitudes[totalSolicitudes]
                    );

                    totalSolicitudes++;
                }
            }
        }

        if (totalSolicitudes > 0) {
            MPI_Waitall(
                totalSolicitudes,
                solicitudes,
                MPI_STATUSES_IGNORE
            );
        }

        tiempoParalelo =
            MPI_Wtime() - inicioTiempo;

        logMaestro << "Tiempo paralelo: "
            << tiempoParalelo << "\n";
        logMaestro << "Total revisadas: "
            << totalRevisadas << "\n";
        logMaestro << "Nodo ganador: "
            << nodoEncontrado << "\n";

        logMaestro.close();

        int ordenFinalizar = 1;

        for (int nodo = 1; nodo < procesos; nodo++) {
            MPI_Send(
                &ordenFinalizar,
                1,
                MPI_INT,
                nodo,
                TAG_FINALIZAR,
                MPI_COMM_WORLD
            );
        }

        cout << "\nRESULTADOS DE BUSQUEDA DISTRIBUIDA\n";

        if (encontrado) {
            cout << "Clave encontrada: "
                << clave << "\n";
            cout << "Indice encontrado: "
                << indiceEncontrado << "\n";
            cout << "Nodo ganador: "
                << nodoEncontrado << "\n";
            cout << "Hilo ganador: "
                << hiloEncontrado << "\n";
        }
        else {
            cout << "Clave no encontrada.\n";
        }

        cout << "Combinaciones revisadas: "
            << totalRevisadas << "\n";
        cout << "Tiempo paralelo: "
            << tiempoParalelo << " segundos\n";

        delete[] solicitudes;
    }
    else {
        ResultadoBusqueda resultado =
            buscarEnRango(clave, inicio, fin, rank);

        unsigned long long datos[5] = {
            resultado.indice,
            resultado.revisadas,
            static_cast<unsigned long long>(
                resultado.hilo < 0 ? 0 : resultado.hilo
            ),
            static_cast<unsigned long long>(
                resultado.encontrado
            ),
            static_cast<unsigned long long>(
                resultado.cancelado
            )
        };

        MPI_Send(
            datos,
            5,
            MPI_UNSIGNED_LONG_LONG,
            0,
            TAG_RESULTADO,
            MPI_COMM_WORLD
        );

        bool finalizar = false;

        while (!finalizar) {
            int orden = 0;
            MPI_Status estado;

            MPI_Recv(
                &orden,
                1,
                MPI_INT,
                0,
                MPI_ANY_TAG,
                MPI_COMM_WORLD,
                &estado
            );

            if (estado.MPI_TAG == TAG_FINALIZAR) {
                finalizar = true;
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
}

void mostrarComparativa(
    double tiempoSecuencial,
    double tiempoParalelo
) {
    if (tiempoSecuencial <= 0 ||
        tiempoParalelo <= 0) {

        cout << "\nPrimero ejecuta las opciones "
            << "2 y 3 con la misma clave.\n";
        return;
    }

    double speedup =
        tiempoSecuencial / tiempoParalelo;

    cout << "\nCOMPARATIVA DE RENDIMIENTO\n";
    cout << fixed << setprecision(8);

    cout << "Tiempo secuencial: "
        << tiempoSecuencial << " segundos\n";
    cout << "Tiempo paralelo: "
        << tiempoParalelo << " segundos\n";
    cout << "Speedup: "
        << speedup << "x\n";

    if (speedup > 1.0) {
        cout << "La busqueda distribuida "
            << "fue mas rapida.\n";
    }
    else {
        cout << "La busqueda secuencial "
            << "fue mas rapida en esta prueba.\n";
    }

    cout << defaultfloat;
}

int main(int argc, char* argv[]) {
    int nivelMPI = 0;

    MPI_Init_thread(
        &argc,
        &argv,
        MPI_THREAD_FUNNELED,
        &nivelMPI
    );

    if (nivelMPI < MPI_THREAD_FUNNELED) {
        cerr << "Error: MPI no ofrece "
            << "el nivel de hilos requerido.\n";

        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    int rank = 0;
    int procesos = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &procesos);

    int opcion = 0;
    string clave;

    double tiempoSecuencial = 0;
    double tiempoParalelo = 0;

    if (rank == 0) {
        mostrarIntegrantes();

        cout << "\nNodo maestro iniciado.\n";
        cout << "Equipo: "
            << obtenerNombreEquipo() << "\n";
        cout << "Procesos MPI: "
            << procesos << "\n";
        cout << "Hilos OpenMP disponibles: "
            << omp_get_max_threads() << "\n";
    }

    do {
        if (rank == 0) {
            mostrarMenu();

            if (!(cin >> opcion)) {
                cin.clear();

                cin.ignore(
                    numeric_limits<streamsize>::max(),
                    '\n'
                );

                opcion = 0;
            }
        }

        MPI_Bcast(
            &opcion,
            1,
            MPI_INT,
            0,
            MPI_COMM_WORLD
        );

        switch (opcion) {
        case 1:
            if (rank == 0) {
                cout << "\nIngresa una clave (A-Z, 0-9): ";
                cin >> clave;

                if (validarClave(clave)) {
                    int longitud =
                        static_cast<int>(clave.length());

                    cout << "Clave aceptada: "
                        << clave << "\n";
                    cout << "Longitud: "
                        << longitud << "\n";
                    cout << "Combinaciones posibles: "
                        << calcularCombinaciones(longitud)
                        << "\n";

                    tiempoSecuencial = 0;
                    tiempoParalelo = 0;
                }
                else {
                    cout << "Clave invalida. "
                        << "Usa de 1 a 10 caracteres "
                        << "A-Z y 0-9.\n";

                    clave.clear();
                    tiempoSecuencial = 0;
                    tiempoParalelo = 0;
                }
            }
            break;

        case 2:
            if (rank == 0) {
                busquedaSecuencial(
                    clave,
                    tiempoSecuencial
                );
            }
            break;

        case 3:
            busquedaDistribuida(
                clave,
                rank,
                procesos,
                tiempoParalelo
            );
            break;

        case 4:
            if (rank == 0) {
                mostrarComparativa(
                    tiempoSecuencial,
                    tiempoParalelo
                );
            }
            break;

        case 5:
            if (rank == 0) {
                cout << "\nFinalizando programa...\n";
                mostrarIntegrantes();
            }
            break;

        default:
            if (rank == 0) {
                cout << "\nOpcion no valida.\n";
            }
            break;
        }

    } while (opcion != 5);

    MPI_Finalize();
    return 0;
}
